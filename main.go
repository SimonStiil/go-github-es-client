package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	//	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/spf13/viper"
)

var (
	logger         *slog.Logger
	debugLogger    *slog.Logger
	configFileName string
	config         *ConfigType
	endpoint       = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "endpoint",
		Help: "The amount of requests to an endpoint",
	}, []string{"path", "status"},
	)
	search       *Search
	twoHourTick  = time.NewTicker(2 * time.Hour)
	quit         = make(chan struct{})
	requests     = 0
	lastRequests = 0
	lastPrint    = time.Now()
)

const (
	BaseENVname = "HOOK"
	webhookPath = "/results"
)

type ConfigType struct {
	Logging    ConfigLogging    `mapstructure:"logging"`
	Port       string           `mapstructure:"port"`
	Prometheus ConfigPrometheus `mapstructure:"prometheus"`
	Elastic    *ConfigElastic   `mapstructure:"elastic"`
}
type ConfigLogging struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}
type ConfigPrometheus struct {
	Enabled  bool   `mapstructure:"enabled"`
	Endpoint string `mapstructure:"endpoint"`
}

func ConfigRead(configFileName string, configOutput *ConfigType) *viper.Viper {
	configReader := viper.New()
	configReader.SetConfigName(configFileName)
	configReader.SetConfigType("yaml")
	configReader.AddConfigPath("/app/")
	configReader.AddConfigPath(".")
	configReader.SetEnvPrefix(BaseENVname)
	configReader.SetDefault("logging.level", "info")
	configReader.SetDefault("logging.format", "text")
	configReader.SetDefault("port", 8080)
	configReader.SetDefault("prometheus.enabled", true)
	configReader.SetDefault("prometheus.endpoint", "/metrics")
	configReader.SetDefault("elastic.addresses", []string{"http://localhost:9200"})
	configReader.SetDefault("elastic.username", "github-hook")
	configReader.SetDefault("elastic.enableMetrics", true)
	configReader.SetDefault("elastic.enableDebugLogging", true)
	configReader.SetDefault("elastic.index", "application-github-webhook-test")

	err := configReader.ReadInConfig() // Find and read the config file
	if err != nil {                    // Handle errors reading the config file
		panic(fmt.Errorf("fatal error config file: %w", err))
	}
	configReader.AutomaticEnv()
	configReader.Unmarshal(configOutput)
	return configReader
}
func setupLogging(Logging ConfigLogging) {
	logLevel := strings.ToLower(Logging.Level)
	logFormat := strings.ToLower(Logging.Format)
	loggingLevel := new(slog.LevelVar)
	switch logLevel {
	case "debug":
		loggingLevel.Set(slog.LevelDebug)
	case "warn":
		loggingLevel.Set(slog.LevelWarn)
	case "error":
		loggingLevel.Set(slog.LevelError)
	default:
		loggingLevel.Set(slog.LevelInfo)
	}

	output := os.Stdout
	switch logFormat {
	case "json":
		logger = slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{Level: loggingLevel}))
		debugLogger = slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{Level: loggingLevel, AddSource: true}))
	default:
		logger = slog.New(slog.NewTextHandler(output, &slog.HandlerOptions{Level: loggingLevel}))
		debugLogger = slog.New(slog.NewTextHandler(output, &slog.HandlerOptions{Level: loggingLevel, AddSource: true}))
	}
	logger.Info("Logging started with options", "format", Logging.Format, "level", Logging.Level, "function", "setupLogging")
	slog.SetDefault(logger)
}

func main() {
	flag.StringVar(&configFileName, "config", "config", "Use a different config file name")
	flag.Parse()
	config = new(ConfigType)
	ConfigRead(configFileName, config)
	envPassword := os.Getenv(BaseENVname + "_ELASTIC_PASSWORD")
	if envPassword != "" {
		config.Elastic.Password = envPassword
	}
	setupLogging(config.Logging)
	search = initSearch(config.Elastic)
	http.HandleFunc(webhookPath, func(w http.ResponseWriter, r *http.Request) {
		requests += 1
		res, err := GetPRData()
		if err != nil {
			endpoint.WithLabelValues(webhookPath, "ESErr").Inc()
			logger.Error("error", "msg", err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(http.StatusText(http.StatusInternalServerError)))
			return
		}
		endpoint.WithLabelValues(webhookPath, "OK").Inc()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(res)
	})
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{\"Status\": \"UP\"}"))
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Webhook Server go to /webhook"))
	})
	if config.Prometheus.Enabled {
		http.Handle(config.Prometheus.Endpoint, promhttp.Handler())
	}
	portString := fmt.Sprintf(":%v", config.Port)
	logger.Info("listeining on port " + portString)

	defer close(quit)
	go Ticker()

	http.ListenAndServe(portString, nil)
}

type ResultsReply struct {
	User    UserReply   `json:"user"`
	Bot     UserReply   `json:"bot"`
	Details []PRDetails `json:"details"`
	Took    string      `json:"took"`
}

type UserReply struct {
	Open   int `json:"open"`
	Closed int `json:"closed"`
}

type PRDetails struct {
	ID       int    `json:"id,omitempty"`
	Number   int    `json:"number,omitempty"`
	State    string `json:"state,omitempty"`
	FullName string `json:"full_name,omitempty"`
	Title    string `json:"title,omitempty"`
}

func GetPRData() (*ResultsReply, error) {
	start := time.Now()
	res, err := search.AllPRs()
	if err != nil {
		return nil, err
	}
	debugLogger.Debug("getting all prs", "count", len(res.Results))
	prs := make(map[int][]Result)
	for _, result := range res.Results {
		prs[result.ID] = append(prs[result.ID], result)
	}
	reply := &ResultsReply{Details: []PRDetails{}}
	for id, results := range prs {
		pr_open := true
		pr_bot := false
		details := PRDetails{}
		for idx, result := range results {
			if idx == 0 {
				details.ID = result.ID
				details.FullName = result.FullName
				details.Title = result.Title
				details.Number = result.Number
			}
			if result.UserType == "Bot" {
				pr_bot = true
			}
			if result.State == "closed" {
				pr_open = false
			}
		}
		debugLogger.Debug("handeling prs", "id", id, "count", len(results), "open", pr_open, "bot", pr_bot)
		if pr_open {
			if pr_bot {
				reply.Bot.Open += 1
			} else {
				reply.User.Open += 1
			}
			details.State = "open"
		} else {
			if pr_bot {
				reply.Bot.Closed += 1
			} else {
				reply.User.Closed += 1
			}
			details.State = "closed"
		}
		reply.Details = append(reply.Details, details)
	}
	reply.Took = time.Since(start).String()
	return reply, nil
}

func Ticker() {
	for {
		select {
		case <-twoHourTick.C:
			debugLogger.Debug("periodic cleanup started.")
			DeleteOldPrs()
			if requests != lastRequests {
				count := requests - lastRequests
				logger.Info("requests", "since-print", time.Since(lastPrint).String(), "count", count)
				lastRequests = requests
				lastPrint = time.Now()
			}
		case <-quit:
			logger.Info("ending ticker")
			twoHourTick.Stop()
			return
		}
	}
}

func DeleteOldPrs() {
	res, err := search.ClosedOldPRs()
	if err != nil {
		logger.Error("error doing search", "error", err)
		return
	}
	for _, result := range res.Results {
		logger.Info("deleting documents relating to closed PR", "document-id", result.DocumentID, "pr-id", result.ID, "full_name", result.FullName, "number", result.Number)
		res, err := search.SearchPR(result.ID)
		if err != nil {
			logger.Error("error doing search", "error", err)
			return
		}
		for _, result := range res.Results {
			search.Delete(result.DocumentID)
			debugLogger.Debug("document deleted", "document-id", result.DocumentID, "pr-id", result.ID)
		}
	}
}
