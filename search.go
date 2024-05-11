package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type ConfigElastic struct {
	Addresses         []string `mapstructure:"addresses"`
	Username          string   `mapstructure:"username"`
	Password          string   `mapstructure:"password"`
	CACert            string   `mapstructure:"cacert"`
	EnableMetrics     bool     `mapstructure:"enableMetrics"`
	EnableDebugLogger bool     `mapstructure:"enableDebugLogging"`
	Index             string   `mapstructure:"index"`
}

func (cfg *ConfigElastic) getConfig() *elasticsearch.Config {
	debugLogger.Debug("reading Elatic search config")
	if cfg.Password == "" {
		debugLogger.Debug("Password empty?")
	}
	config := &elasticsearch.Config{
		Addresses:         cfg.Addresses,
		Username:          cfg.Username,
		Password:          cfg.Password,
		EnableMetrics:     cfg.EnableMetrics,
		EnableDebugLogger: cfg.EnableDebugLogger,
	}
	if cfg.CACert != "" {
		sDec, err := base64.StdEncoding.DecodeString(cfg.CACert)
		if err != nil {
			logger.Error("error decoding base64", "error", err)
			os.Exit(1)
		}
		config.CACert = sDec
	}
	return config
}

func printESError(message string, res *esapi.Response) {
	bodyText, err := io.ReadAll(res.Body)
	if err != nil {
		logger.Error("error reading body", "error", err)
	}
	var e map[string]interface{}
	err = json.Unmarshal(bodyText, &e)
	if err != nil {
		logger.Error("error unmarshaling body", "body", bodyText, "error", err)
	}
	logger.Error(message, "status", res.Status(), "type", e["error"].(map[string]interface{})["type"], "reason", e["error"].(map[string]interface{})["reason"])
}

type Search struct {
	esClient *elasticsearch.Client
	index    string
}

func initSearch(config *ConfigElastic) *Search {
	var err error
	search := &Search{index: config.Index}
	search.esClient, err = elasticsearch.NewClient(*config.getConfig())
	if err != nil {
		logger.Error("error staring elasticsearch client", "error", err)
		os.Exit(1)
	}
	if err != nil {
		logger.Error("error opening mappings file", "error", err)
		os.Exit(2)
	}
	res, err := search.esClient.Indices.Exists([]string{config.Index})
	if err != nil {
		logger.Error("error creating indice", "error", err)
	}
	if res.StatusCode == http.StatusNotFound {
		printESError("Indice does not exist, is webhook installed?", res)
		os.Exit(3)
	} else {
		if res.StatusCode == http.StatusOK {
			logger.Info("Indice exists, starting")
		} else {
			if res.StatusCode == http.StatusUnauthorized {
				printESError("Elastic Connection Unauthorized", res)
				os.Exit(-1)
			} else {
				printESError("Unknown response", res)
				os.Exit(4)
			}
		}
	}
	return search
}

type EnvelopeResponse struct {
	Took     int  `json:"took"`
	TimedOut bool `json:"timed_out"`
	Hits     struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []struct {
			ID         string          `json:"_id"`
			Source     json.RawMessage `json:"_source,omitempty"`
			Index      string          `json:"_index"`
			Highlights json.RawMessage `json:"highlight,omitempty"`
			Sort       []interface{}   `json:"sort"`
			Fields     struct {
				Number   []int    `json:"number"`
				Title    []string `json:"pull_request.title"`
				FullName []string `json:"pull_request.head.repo.full_name"`
				UserType []string `json:"pull_request.user.type"`
				Action   []string `json:"action"`
				State    []string `json:"pull_request.state"`
				ID       []int    `json:"pull_request.id"`
			} `json:"fields"`
		}
	} `json:"hits"`
}

type Message struct {
	Query  Query               `json:"query,omitempty"`
	Sort   []map[string]string `json:"sort,omitempty"`
	Fields []string            `json:"fields,omitempty"`
	Source bool                `json:"_source,omitempty"`
}

type Query struct {
	Match    *map[string]string `json:"match,omitempty"`
	MatchAll *map[string]string `json:"match_all,omitempty"`
	Bool     *Bool              `json:"bool,omitempty"`
}

type Bool struct {
	MustNot *Term          `json:"must_not,omitempty"`
	Filter  *[]interface{} `json:"filter,omitempty"`
}

type Term struct {
	Term map[string]string `json:"term,omitempty"`
}
type Range struct {
	Range map[string]RangeFilter `json:"range,omitempty"`
}
type RangeFilter struct {
	GreaterThen       *string `json:"gt,omitempty"`
	GreaterThenEquals *string `json:"gte,omitempty"`
	LessThen          *string `json:"lt,omitempty"`
	LessThenEquals    *string `json:"lte,omitempty"`
}

type SearchResults struct {
	Total   int      `json:"total"`
	Results []Result `json:"result"`
}

type Result struct {
	DocumentID string `json:"_id,omitempty"`
	ID         int    `json:"id,omitempty"`
	Number     int    `json:"number,omitempty"`
	State      string `json:"state,omitempty"`
	Action     string `json:"action,omitempty"`
	FullName   string `json:"full_name,omitempty"`
	UserType   string `json:"user_type,omitempty"`
	Title      string `json:"title,omitempty"`
}

func (search *Search) search(message *Message) (*SearchResults, error) {
	var results SearchResults
	jsonMessageBytes, err := json.Marshal(message)
	if err != nil {
		logger.Error("error Marshaling message", "message", message, "error", err)
	}
	jsonMessage := string(jsonMessageBytes)
	debugLogger.Debug(jsonMessage)
	res, err := search.esClient.Search(
		search.esClient.Search.WithIndex(search.index),
		search.esClient.Search.WithBody(strings.NewReader(jsonMessage)),
	)
	if err != nil {
		return &results, err
	}
	defer res.Body.Close()

	if res.IsError() {
		printESError("Error doing search", res)
	}

	var r EnvelopeResponse

	bodyText, err := io.ReadAll(res.Body)
	if err != nil {
		logger.Error("error reading body", "error", err)
	}
	//logger.Debug(string(bodyText))
	if err := json.Unmarshal(bodyText, &r); err != nil {
		return &results, err
	}
	results.Total = r.Hits.Total.Value
	debugLogger.Debug("hits", "Total.Value", r.Hits.Total.Value, "len(Hits)", len(r.Hits.Hits))
	if len(r.Hits.Hits) < 1 {
		results.Results = []Result{}
		return &results, nil
	}
	for _, hit := range r.Hits.Hits {
		var result Result
		result.DocumentID = hit.ID
		result.Action = flattenStringList(hit.Fields.Action)
		result.FullName = flattenStringList(hit.Fields.FullName)
		result.ID = hit.Fields.ID[0]
		result.Number = hit.Fields.Number[0]
		result.State = flattenStringList(hit.Fields.State)
		result.Title = flattenStringList(hit.Fields.Title)
		result.UserType = flattenStringList(hit.Fields.UserType)
		results.Results = append(results.Results, result)
	}
	return &results, nil
}

func flattenStringList(list []string) string {
	var res string
	for idx, val := range list {
		if idx == 0 {
			res = val
		} else {
			res = fmt.Sprintf("%v, %v", res, val)
		}
	}
	return res
}

var (
	closedPRsMessage = &Message{
		Query: Query{
			Match: &map[string]string{
				"action": "closed",
			}},
		Sort: []map[string]string{
			{"pull_request.head.repo.full_name": "asc"},
			{"timestamp": "desc"},
			{"pull_request.id": "asc"},
		},
		Fields: []string{
			"number",
			"action",
			"pull_request.id",
			"pull_request.state",
			"pull_request.title",
			"pull_request.head.repo.full_name",
			"pull_request.user.type"},
		Source: false,
	}
	yearBack            = "now-365d/d"
	dayBack             = "now-1d/d"
	closedOldPRsMessage = &Message{
		Query: Query{
			Bool: &Bool{Filter: &[]interface{}{
				&Term{Term: map[string]string{"action": "closed"}},
				&Range{Range: map[string]RangeFilter{"timestamp": {
					GreaterThenEquals: &yearBack,
					LessThenEquals:    &dayBack,
				}}},
			}}},
		Sort: []map[string]string{
			{"pull_request.head.repo.full_name": "asc"},
			{"timestamp": "desc"},
			{"pull_request.id": "asc"},
		},
		Fields: []string{
			"number",
			"action",
			"pull_request.id",
			"pull_request.state",
			"pull_request.title",
			"pull_request.head.repo.full_name",
			"pull_request.user.type"},
		Source: false,
	}
	allPRsMessage = &Message{
		Query: Query{
			MatchAll: &map[string]string{},
		},
		Sort: []map[string]string{
			{"pull_request.head.repo.full_name": "asc"},
			{"pull_request.id": "desc"},
			{"timestamp": "desc"},
		},
		Fields: []string{
			"number",
			"action",
			"pull_request.id",
			"pull_request.state",
			"pull_request.title",
			"pull_request.head.repo.full_name",
			"pull_request.user.type"},
		Source: false,
	}
	openPRsMessage = &Message{
		Query: Query{
			Bool: &Bool{
				MustNot: &Term{
					Term: map[string]string{
						"action": "closed",
					}}},
		},
		Sort: []map[string]string{
			{"pull_request.head.repo.full_name": "asc"},
			{"timestamp": "desc"},
			{"pull_request.id": "asc"},
		},
		Fields: []string{
			"number",
			"action",
			"pull_request.id",
			"pull_request.state",
			"pull_request.title",
			"pull_request.head.repo.full_name",
			"pull_request.user.type"},
		Source: false,
	}
)

func (search *Search) AllPRs() (*SearchResults, error) {
	return search.search(allPRsMessage)
}
func (search *Search) ClosedPRs() (*SearchResults, error) {
	return search.search(closedPRsMessage)
}
func (search *Search) ClosedOldPRs() (*SearchResults, error) {
	return search.search(closedOldPRsMessage)
}
func (search *Search) OpenPRs() (*SearchResults, error) {
	return search.search(openPRsMessage)
}
func (search *Search) SearchPR(id int) (*SearchResults, error) {
	searchMessage := &Message{
		Query: Query{
			Match: &map[string]string{
				"pull_request.id": fmt.Sprintf("%v", id),
			}},
		Sort: []map[string]string{
			{"pull_request.head.repo.full_name": "asc"},
			{"timestamp": "desc"},
		},
		Fields: []string{
			"number",
			"action",
			"pull_request.id",
			"pull_request.state",
			"pull_request.title",
			"pull_request.head.repo.full_name"},
		Source: false,
	}
	return search.search(searchMessage)
}

func (search *Search) Delete(documentID string) {
	ctx := context.Background()
	res, err := esapi.DeleteRequest{Index: search.index, DocumentID: documentID}.Do(ctx, search.esClient)
	if err != nil {
		logger.Error("error doing delete request", "error", err)
		return
	}
	if res.IsError() {
		printESError("error posting value", res)
		return
	}
	debugLogger.Debug("Deleted", "document", documentID)
}
