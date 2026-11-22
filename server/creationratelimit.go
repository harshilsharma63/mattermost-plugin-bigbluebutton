package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
)

const meetingCreationLimit = 10

// allowMeetingCreation allows ten requests per user per fixed minute. Atomic KV
// updates share the limit across plugin instances; expiry bounds stored counters.
func (p *Plugin) allowMeetingCreation(userID string, now time.Time) (bool, int64, error) {
	const windowSeconds = int64(time.Minute / time.Second)
	window := now.Unix() / windowSeconds
	retryAfter := windowSeconds - now.Unix()%windowSeconds
	key := "create_rate_" + userID + "_" + strconv.FormatInt(window, 10)
	for attempt := 0; attempt < meetingCreationLimit; attempt++ {
		oldValue, appErr := p.API.KVGet(key)
		if appErr != nil {
			return false, 0, fmt.Errorf("read creation counter: %w", appErr)
		}
		count := 0
		if len(oldValue) != 0 {
			var err error
			count, err = strconv.Atoi(string(oldValue))
			if err != nil || count < 0 {
				return false, 0, fmt.Errorf("invalid creation counter")
			}
		}
		if count >= meetingCreationLimit {
			return false, retryAfter, nil
		}
		stored, appErr := p.API.KVSetWithOptions(key, []byte(strconv.Itoa(count+1)), model.PluginKVSetOptions{
			Atomic:          true,
			OldValue:        oldValue,
			ExpireInSeconds: 2 * windowSeconds,
		})
		if appErr != nil {
			return false, 0, fmt.Errorf("update creation counter: %w", appErr)
		}
		if stored {
			return true, 0, nil
		}
	}
	// Fail closed under contention instead of allowing an uncounted request.
	return false, retryAfter, nil
}
