package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blindsidenetworks/mattermost-plugin-bigbluebutton/server/bigbluebuttonapiwrapper/dataStructs"
	"github.com/mattermost/mattermost/server/public/model"
)

const (
	creationUserID    = "aaaaaaaaaaaaaaaaaaaaaaaaaa"
	creationChannelID = "bbbbbbbbbbbbbbbbbbbbbbbbbb"
)

type creationTestAPI struct {
	dependencyTestAPI
	member     *model.ChannelMember
	memberErr  *model.AppError
	channel    *model.Channel
	channelErr *model.AppError
	canPost    bool
	postErr    *model.AppError
	storageErr *model.AppError
	writeErr   *model.AppError
	conflict   bool
	users      []string
	alerts     []map[string]any
	warnings   int
	mu         sync.Mutex
	kv         map[string][]byte
}

func newCreationPlugin() (*Plugin, *creationTestAPI) {
	api := &creationTestAPI{
		member:  &model.ChannelMember{UserId: creationUserID, ChannelId: creationChannelID},
		channel: &model.Channel{Id: creationChannelID, Type: model.ChannelTypeOpen},
		canPost: true,
		kv:      make(map[string][]byte),
	}
	p := &Plugin{}
	p.SetAPI(api)
	p.configuration.Store(&Configuration{BaseURL: testBBBURL, Secret: "creation-test-secret", AllowRecordings: true})
	return p, api
}

func (a *creationTestAPI) GetChannelMember(channelID, userID string) (*model.ChannelMember, *model.AppError) {
	if channelID != creationChannelID || userID != creationUserID {
		panic("membership checked for untrusted identity or channel")
	}
	return a.member, a.memberErr
}

func (a *creationTestAPI) GetChannel(id string) (*model.Channel, *model.AppError) {
	if id != creationChannelID {
		panic("unexpected channel")
	}
	return a.channel, a.channelErr
}

func (a *creationTestAPI) HasPermissionToChannel(userID, channelID string, permission *model.Permission) bool {
	if userID != creationUserID || channelID != creationChannelID || permission != model.PermissionCreatePost {
		panic("incorrect posting permission check")
	}
	return a.canPost
}

func (a *creationTestAPI) GetUser(id string) (*model.User, *model.AppError) {
	a.users = append(a.users, id)
	return a.dependencyTestAPI.GetUser(id)
}

func (a *creationTestAPI) CreatePost(post *model.Post) (*model.Post, *model.AppError) {
	if a.postErr != nil {
		return nil, a.postErr
	}
	return a.dependencyTestAPI.CreatePost(post)
}

func (a *creationTestAPI) PublishWebSocketEvent(_ string, payload map[string]any, _ *model.WebsocketBroadcast) {
	a.alerts = append(a.alerts, payload)
}

func (a *creationTestAPI) LogWarn(_ string, _ ...any)  { a.warnings++ }
func (a *creationTestAPI) LogError(_ string, _ ...any) {}

func (a *creationTestAPI) KVGet(key string) ([]byte, *model.AppError) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return bytes.Clone(a.kv[key]), a.storageErr
}

func (a *creationTestAPI) KVSet(key string, value []byte) *model.AppError {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.kv[key] = bytes.Clone(value)
	return nil
}

func (a *creationTestAPI) KVSetWithOptions(key string, value []byte, options model.PluginKVSetOptions) (bool, *model.AppError) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !options.Atomic || options.ExpireInSeconds <= 0 {
		panic("creation counters must be atomic and expire")
	}
	if a.writeErr != nil {
		return false, a.writeErr
	}
	if a.conflict || !bytes.Equal(a.kv[key], options.OldValue) {
		return false, nil
	}
	a.kv[key] = bytes.Clone(value)
	return true, nil
}

func creationRequest(body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/create", strings.NewReader(body))
	r.Header.Set(userIDHeaderName, creationUserID)
	return r
}

func validCreationBody() string {
	return `{"channel_id":"` + creationChannelID + `","title":"Test meeting","allow_recording":true}`
}

func TestRequireAuthStopsHandler(t *testing.T) {
	called := false
	handler := middlewareRequireAuth(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	response := httptest.NewRecorder()
	handler(response, httptest.NewRequest(http.MethodPost, "/create", nil))
	if response.Code != http.StatusUnauthorized || called {
		t.Fatalf("unauthenticated handler ran: status=%d, called=%v", response.Code, called)
	}
	r := creationRequest("")
	handler(httptest.NewRecorder(), r)
	if !called {
		t.Fatal("authenticated handler did not run")
	}
}

func TestCreateMeetingRejectsUnauthorizedRequests(t *testing.T) {
	appErr := model.NewAppError("test", "test", nil, "lookup failed", http.StatusInternalServerError)
	tests := []struct {
		name   string
		change func(*creationTestAPI, *http.Request)
		status int
	}{
		{"unauthenticated", func(_ *creationTestAPI, r *http.Request) { r.Header.Del(userIDHeaderName) }, http.StatusUnauthorized},
		{"wrong method", func(_ *creationTestAPI, r *http.Request) { r.Method = http.MethodGet }, http.StatusMethodNotAllowed},
		{"not a channel member", func(a *creationTestAPI, _ *http.Request) { a.member = nil }, http.StatusForbidden},
		{"membership lookup failed", func(a *creationTestAPI, _ *http.Request) { a.memberErr = appErr }, http.StatusForbidden},
		{"cannot post", func(a *creationTestAPI, _ *http.Request) { a.canPost = false }, http.StatusForbidden},
		{"archived channel", func(a *creationTestAPI, _ *http.Request) { a.channel.DeleteAt = 1 }, http.StatusForbidden},
		{"missing channel", func(a *creationTestAPI, _ *http.Request) { a.channel = nil }, http.StatusForbidden},
		{"channel lookup failed", func(a *creationTestAPI, _ *http.Request) { a.channelErr = appErr }, http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, api := newCreationPlugin()
			r := creationRequest(validCreationBody())
			tt.change(api, r)
			response := httptest.NewRecorder()
			p.GetRouteHandler("/create")(response, r)
			if response.Code != tt.status {
				t.Fatalf("status=%d, want %d: %s", response.Code, tt.status, response.Body)
			}
			if api.post != nil || len(api.kv) != 0 || len(api.users) != 0 || len(api.alerts) != 0 {
				t.Fatal("rejected request caused creation side effects")
			}
		})
	}
	// Also protect the handler if it is called without the routing middleware.
	p := &Plugin{}
	response := httptest.NewRecorder()
	p.handleCreateMeeting(response, httptest.NewRequest(http.MethodPost, "/create", strings.NewReader(validCreationBody())))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("direct unauthenticated request: %d", response.Code)
	}
}

func TestCreateMeetingRejectsNonMembersOfEveryChannelType(t *testing.T) {
	for _, channelType := range []model.ChannelType{model.ChannelTypeOpen, model.ChannelTypePrivate, model.ChannelTypeDirect, model.ChannelTypeGroup} {
		t.Run(string(channelType), func(t *testing.T) {
			p, api := newCreationPlugin()
			api.channel.Type = channelType
			api.member = nil
			// Even a user with posting permission must be a member.
			response := httptest.NewRecorder()
			p.handleCreateMeeting(response, creationRequest(validCreationBody()))
			if response.Code != http.StatusForbidden || api.post != nil || len(api.kv) != 0 || api.warnings != 1 {
				t.Fatalf("non-member request was not denied and logged: %d", response.Code)
			}
		})
	}
}

func TestCreateMeetingUsesAuthenticatedIdentity(t *testing.T) {
	for _, channelType := range []model.ChannelType{model.ChannelTypeOpen, model.ChannelTypePrivate, model.ChannelTypeDirect, model.ChannelTypeGroup} {
		for _, suppliedUser := range []string{"", creationUserID, "cccccccccccccccccccccccccc"} {
			t.Run(string(channelType)+"/"+suppliedUser, func(t *testing.T) {
				p, api := newCreationPlugin()
				api.channel.Type = channelType
				api.channel.Name = creationUserID + "__other-user"
				body := strings.TrimSuffix(validCreationBody(), "}")
				if suppliedUser != "" {
					body += `,"user_id":"` + suppliedUser + `"`
				}
				response := httptest.NewRecorder()
				p.GetRouteHandler("/create")(response, creationRequest(body+"}"))
				if response.Code != http.StatusOK || api.post == nil {
					t.Fatalf("create failed: %d: %s", response.Code, response.Body)
				}
				if api.post.UserId != creationUserID || api.post.ChannelId != creationChannelID || api.post.GetProp(propKeyCreatedBy) != creationUserID {
					t.Fatalf("incorrect post attribution: %+v", api.post)
				}
				for _, id := range api.users {
					if id != creationUserID {
						t.Fatalf("looked up body-supplied identity %q", id)
					}
				}
				var meeting dataStructs.MeetingRoom
				meetingID := api.post.GetProp(propKeyMeetingID).(string)
				if err := json.Unmarshal(api.kv[prefixMeeting+meetingID], &meeting); err != nil {
					t.Fatal(err)
				}
				if meeting.Meta_dc_creator != "alice@example.com" || meeting.Meta_channelid != creationChannelID || meeting.PostId != api.post.Id {
					t.Fatalf("incorrect meeting attribution: %+v", meeting)
				}
				if channelType == model.ChannelTypeDirect {
					if len(api.alerts) != 1 || api.alerts[0]["fromUserID"] != creationUserID {
						t.Fatalf("incorrect call attribution: %v", api.alerts)
					}
				}
			})
		}
	}
}

func TestCreateMeetingRejectsMalformedBody(t *testing.T) {
	bodies := []string{
		"", "{", "null", "[]", "{}", `{"channel_id":"invalid"}`,
		validCreationBody() + "{}", strings.Repeat(" ", 64*1024) + validCreationBody(),
	}
	for _, body := range bodies {
		t.Run(strconv.Itoa(len(body))+body[:min(len(body), 20)], func(t *testing.T) {
			// No API set: validation must reject before any API calls.
			p := &Plugin{}
			response := httptest.NewRecorder()
			p.handleCreateMeeting(response, creationRequest(body))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("invalid body returned %d", response.Code)
			}
		})
	}
}

func TestCreateMeetingStopsOnPostFailure(t *testing.T) {
	p, api := newCreationPlugin()
	api.postErr = model.NewAppError("test", "test", nil, "post failed", http.StatusInternalServerError)
	response := httptest.NewRecorder()
	p.handleCreateMeeting(response, creationRequest(validCreationBody()))
	if response.Code != http.StatusInternalServerError || len(api.alerts) != 0 {
		t.Fatalf("post failure status=%d, alerts=%v", response.Code, api.alerts)
	}
	for key := range api.kv {
		if strings.HasPrefix(key, prefixMeeting) {
			t.Fatalf("saved meeting after post failure: %s", key)
		}
	}
}

func TestMeetingCreationRateLimit(t *testing.T) {
	p, api := newCreationPlugin()
	// A second plugin process shares the same KV backend.
	other := &Plugin{}
	other.SetAPI(api)
	now := time.Unix(120, 0)
	var allowed atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Go(func() {
			ok, _, err := other.allowMeetingCreation(creationUserID, now)
			if err != nil {
				t.Error(err)
			}
			if ok {
				allowed.Add(1)
			}
		})
	}
	wg.Wait()
	if allowed.Load() != meetingCreationLimit {
		t.Fatalf("concurrent creation count=%d, want %d", allowed.Load(), meetingCreationLimit)
	}
	ok, retry, err := p.allowMeetingCreation(creationUserID, now.Add(30*time.Second))
	if ok || retry != 30 || err != nil {
		t.Fatalf("rate limit=%v, retry=%d, error=%v", ok, retry, err)
	}
	for _, attempt := range []struct {
		user string
		time time.Time
	}{{"another-user", now}, {creationUserID, now.Add(time.Minute)}} {
		ok, _, err = p.allowMeetingCreation(attempt.user, attempt.time)
		if !ok || err != nil {
			t.Fatalf("independent user/window blocked: %v", err)
		}
	}
}

func TestCreateMeetingRateLimitResponses(t *testing.T) {
	for _, test := range []string{"limit exceeded", "storage failure", "storage write failure", "contention"} {
		t.Run(test, func(t *testing.T) {
			p, api := newCreationPlugin()
			status := http.StatusTooManyRequests
			switch test {
			case "limit exceeded":
				for i := 0; i < meetingCreationLimit; i++ {
					ok, _, err := p.allowMeetingCreation(creationUserID, time.Now())
					if !ok || err != nil {
						t.Fatalf("unable to fill limiter: %v", err)
					}
				}
			case "storage failure":
				api.storageErr = model.NewAppError("test", "test", nil, "storage unavailable", http.StatusInternalServerError)
				status = http.StatusServiceUnavailable
			case "storage write failure":
				api.writeErr = model.NewAppError("test", "test", nil, "storage unavailable", http.StatusInternalServerError)
				status = http.StatusServiceUnavailable
			case "contention":
				api.conflict = true
			}
			response := httptest.NewRecorder()
			p.handleCreateMeeting(response, creationRequest(validCreationBody()))
			if response.Code != status || api.post != nil || len(api.users) != 0 || len(api.alerts) != 0 {
				t.Fatalf("limiter response=%d, want %d", response.Code, status)
			}
			if status == http.StatusTooManyRequests {
				retry, err := strconv.Atoi(response.Header().Get("Retry-After"))
				if err != nil || retry < 1 || retry > 60 || api.warnings != 1 {
					t.Fatalf("invalid retry header or missing warning: %q", response.Header().Get("Retry-After"))
				}
			}
		})
	}
}
