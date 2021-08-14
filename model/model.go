package model

import (
	"fmt"
	"github.com/gin-gonic/gin/binding"
	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
	"gorm.io/datatypes"

)

type ActivityType int

const (
	// Event 是到某某时间段进行的事件
	Event ActivityType = iota // 事件
	// Job 是到某某时刻触发任务
	Job // 任务
)

type ActivityUpdateMap struct {
	ActivityKey string                 `binding:"required" json:"activity_key"` // 键
	UpdateMap   map[string]interface{} `json:"update_map"`                      // 更新Map
}
type Activity struct {
	ActivityKey      string         `gorm:"column:activity_key;primary_key;type:varchar(255);comment:'coding的任务号'" binding:"required" json:"activity_key"` // 键
	StartTime        int64   `gorm:"column:start_time;type:int(10);comment:'活动开始时间'" binding:"required" json:"start_time"`                          // 活动开始时间戳
	MT               bool           `gorm:"column:mt" json:"mt"`                                                                                           //    如果是手动调整数据库的话直接改这个字段就行  不用把开始时间调整为过去时间 0是自动 1是手动
	Schedule         string         `gorm:"column:schedule;type:varchar(255)"  json:"schedule"`                                                            // https://pkg.go.dev/github.com/robfig/cron/v3
	LastScheduleTime int64   `gorm:"column:last_schedule_time;type:int(10)"  json:"last_schedule_time"`                                             // 最后一次调度时间戳
	LastScheduleResp string         `gorm:"column:last_schedule_resp"  json:"last_schedule_resp"`                                                          // 最后一次调度response
	Type             ActivityType   ` gorm:"column:type" json:"type"`                                                                                      // 活动类型 前端只能更新任务类型
	Body             datatypes.JSON `gorm:"column:body" json:"body"`                                                                                       // 请求Body
	Finished         bool           `gorm:"column:finished" json:"finished"`                                                                               //   如果不想让任务继续触发 设置为true 或者当mt为1时 运行一次之后会设置成true 如果需要继续自动运行 需要把finish设置为false
}

func (a Activity) Validate() error {
	if a.Schedule != "" {
		_, err := cron.ParseStandard(a.Schedule)
		if err != nil {
			return err
		}
	}

	return binding.Validator.ValidateStruct(a)
}
func (a Activity) TableName() string {
	return "acw_activity"
}

// Error represents an error returned in a command reply. redigo 的切分方法
type Error string

func (err Error) Error() string { return string(err) }

// Strings is a helper that converts an array command reply to a []string. If
// err is not equal to nil, then Strings returns nil, err. Nil array items are
// converted to "" in the output slice. Strings returns an error if an array
// item is not a bulk string or nil.
func Strings(reply interface{}, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	switch reply := reply.(type) {
	case []interface{}:
		result := make([]string, len(reply))
		for i := range reply {
			if reply[i] == nil {
				continue
			}
			p, ok := reply[i].(string)
			if !ok {
				return nil, fmt.Errorf("redigo: unexpected element type for Strings, got type %T", reply[i])
			}
			result[i] = p
		}
		return result, nil
	case nil:
		return nil, errors.New("redigo: nil returned")
	case Error:
		return nil, reply
	}
	return nil, fmt.Errorf("redigo: unexpected type for Strings, got type %T", reply)
}
