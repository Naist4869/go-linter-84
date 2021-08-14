package activity

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go-linter-84/helpers"
	"go-linter-84/helpers/wait"
	"go-linter-84/helpers/workqueue"
	"go-linter-84/model"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"time"

	"github.com/casbin/casbin/v2"
	"github.com/robfig/cron/v3"

	"github.com/gin-gonic/gin"

	"gorm.io/gorm"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

// 空对象
const cacheNull = "{null}"

// FGetKey 根据id获取key
type FGetKey func(id string) (key string, field []string)

type service struct {
	redis       *redis.Client
	mysql       *gorm.DB
	cacheCh     chan map[string]interface{} // 服务结束关闭channel  现在不用
	stopCh      chan struct{}
	controller  *Controller
	old         []*model.Activity
	adminRouter gin.IRoutes
}

func (s *service) Close() {
	s.controller.queue.ShutDown()
	close(s.stopCh)
	close(s.cacheCh)
}

func (s *service) IsClosed() bool {
	if s.controller.queue.ShuttingDown() {
		_, open := <-s.stopCh
		if !open {
			_, open := <-s.cacheCh
			if !open {
				return true
			}
		}
	}
	return false
}

func (s *service) Key() string {
	return "activity"
}

func (s *service) Name() string {
	return "活动管理"
}

func (s *service) Acting(ctx context.Context, key string) bool {
	objDest := &keysToActivityArr{}
	if err := s.objCache(ctx, []string{key}, objDest); err != nil {
		return false
	}
	for _, activity := range objDest.dest {
		if !activity.Finished {
			if activity.ActivityKey == key {
				return true
			}
		}
	}
	return false
}

func NewService(redisClient *redis.Client, mysql *gorm.DB, host string, engine *gin.Engine, enforcer casbin.IEnforcer) (*service, error) {

	s := &service{
		redis:       redisClient,
		mysql:       mysql,
		cacheCh:     make(chan map[string]interface{}, 10240),
		stopCh:      make(chan struct{}, 0),
		adminRouter: engine.Group("/admin/activity"),
	}
	err := s.mysql.AutoMigrate(&model.Activity{})
	go func() {
		defer func() {
			if e := recover(); e != nil {
			}
		}()
		s.setCache()
	}()
	s.controller = NewController(s, host, s.stopCh)
	return s, err
}

func (s *service) setCache() {
	for {
		missCache, open := <-s.cacheCh
		if !open {
			break
		}
		for key, m := range missCache {
			ctx := context.Background()
			if _, err := s.redis.HSet(ctx, key, m).Result(); err != nil {
			}
			// 这里应该用事务
			ttl, _ := s.redis.TTL(ctx, key).Result()
			if ttl < time.Second {
				// 活不长了 或者是第一次设置
				// 设置下10秒清理一次 再从数据库拿 当我们修改数据库的时候可以保证9秒内生效
				s.redis.Expire(ctx, key, time.Second*10)
			}
		}
		// 防止redis阻塞
		time.Sleep(time.Millisecond * 1)
	}
}

func (s *service) getActivities(ctx context.Context, keys []string) (activities []*model.Activity, err error) {
	if len(keys) == 0 {
		return
	}
	db := s.mysql.Model(&model.Activity{}).WithContext(ctx)
	if err = db.Where("activity_key in ?", keys).Where(db.Or("mt = true").Or("start_time < ?", time.Now().Unix()).Or("type = ?", model.Job)).Find(&activities).Error; err != nil {
		return
	}

	if len(activities) == 0 {

		err = fmt.Errorf("keys: %v", keys)
	}
	return
}

func (s *service) GetActivities(ctx context.Context, ids []string) (results []*model.Activity, err error) {
	objDest := &keysToActivityArr{}
	if err = s.objCache(ctx, ids, objDest); err != nil {
		return
	}

	return objDest.dest, nil
}

func (s *service) objCache(ctx context.Context, ids []string, dest ICacheObj) (err error) {
	// 从缓存中拿数据
	miss := s.getFromCache(ctx, ids, dest.GetKey, dest)
	if len(miss) == 0 {
		return
	}
	// miss的从mysql填充进缓存
	missCache, err := dest.FillFromDB(ctx, s, miss)
	if err != nil {
		return
	}
	// 设置缓存
	if missCache == nil {
		missCache = map[string]interface{}{}
	}
	var key string
	for _, missid := range miss {
		var fields []string
		key, fields = dest.GetKey(missid)
		for _, missk := range fields {
			if _, exist := missCache[missk]; exist {
				continue
			}
			missCache[missk] = cacheNull
		}
	}
	s.cacheCh <- map[string]interface{}{key: missCache}
	return
}

// ICacheObj 对象缓存接口
type ICacheObj interface {
	// GetKey 获取对象缓存的key
	GetKey(id string) (key string, field []string)
	io.Writer
	FillFromDB(ctx context.Context, d IActivity, miss []string) (missCache map[string]interface{}, err error)
}

var _activity = "activity_map" //  key -> field -> json

// keysToActivityArr 开箱即用
type keysToActivityArr struct {
	dest []*model.Activity
}

func (o *keysToActivityArr) GetKey(id string) (key string, field []string) {
	// 一个是key键  一个是map的键
	return _activity, []string{id}
}

func (o *keysToActivityArr) Write(p []byte) (n int, err error) {
	one := &model.Activity{}
	if err = json.Unmarshal(p, one); err != nil {
		return
	}
	o.dest = append(o.dest, one)
	return
}
func IsGetActivityRecordNotFound(err error) bool {
	return false
}
func (o *keysToActivityArr) FillFromDB(ctx context.Context, d IActivity, miss []string) (missCache map[string]interface{}, err error) {
	activities, err := d.getActivities(ctx, miss)
	if err != nil && !IsGetActivityRecordNotFound(err) {
		err = errors.Wrap(err, fmt.Sprintf("miss: (%s)", miss))
		return
	}
	o.dest = append(o.dest, activities...)
	missCache = map[string]interface{}{}
	for _, item := range activities {
		bs, err := json.Marshal(item)
		if err != nil {
			continue
		}
		_, fields := o.GetKey(item.ActivityKey)
		for _, k := range fields {
			missCache[k] = string(bs)
		}
	}
	return missCache, nil
}

func (s *service) getFromCache(ctx context.Context, ids []string, getKey FGetKey, dest io.Writer) (miss []string) {

	var (
		err          error
		caches       []string
		key          string
		idsInterface interface{}
	)

	if len(ids) == 0 {
		return
	}
	// 去重

	idsInterface, err = helpers.RemoveDuplicate(ids, func(i, j int) bool {
		return ids[i] != "" && ids[j] != "" && ids[i] == ids[j]
	})
	if err != nil {
	}
	ids = idsInterface.([]string)
	keymap := make(map[string]string, len(ids)) // 关键词->id
	fields := make([]string, 0, len(ids))       // []关键字

	for _, id := range ids {
		var field []string
		key, field = getKey(id)

		for _, k := range field {
			if _, exist := keymap[k]; exist {
				continue
			}
			keymap[k] = id
			fields = append(fields, k)
		}
	}

	if caches, err = model.Strings(s.redis.HMGet(ctx, key, fields...).Result()); err != nil {
		miss = ids
		return miss
	}
	// 去重
	missMap := map[string]bool{}
	for i, item := range caches {
		id := keymap[fields[i]]
		if item != "" && item != cacheNull {
			if _, err = dest.Write([]byte(item)); err == nil {
				continue
			}
		}
		if _, exist := missMap[id]; exist || item == cacheNull {
			continue
		}
		miss = append(miss, id)
		missMap[id] = true
	}
	return miss
}

/*-------------------------------定时任务-------------------------------*/

// 目标 1.在数据库变更记录  程序可以感知  目前是通过god接口调用函数  或者通过https://github.com/traefik/yaegi 动态编译成Func类型
// 2. 系统停止时可以追进度 比如说有个任务每天执行一次  数据库存的最后一次调度时间是前天 那么今天系统启动就应该执行两次追上进度然后进入休眠
// 3. 可以随时更改调度时间 采用linux corn格式  比如每天执行更改到每小时执行
// 4. 重试机制 搁置
// 5. job运行时间计时存储入库 只存最后一次  搁置
// 6. 显示有几个任务在跑通知  (打个warn日志)
// 7. 设置已完成的任务不再执行

type CronJobLister interface {
	CornJobWatcher()
	SetLastScheduleTime(activity *model.Activity, lastScheduleTime time.Time)
	SetLastScheduleResp(activity *model.Activity, resp string)
	SetFinishMT(activity *model.Activity)
	GetActivity(ctx context.Context, key string) (result *model.Activity, err error)
}

var (
	nextScheduleDelta = 100 * time.Millisecond
)

type Controller struct {
	queue         workqueue.RateLimitingInterface
	cronJobLister CronJobLister
	httpClient    *http.Client
	host          string
}

func NewController(cronJobLister IActivity, host string, stop <-chan struct{}) *Controller {
	c := &Controller{
		queue:         workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "cronjob"),
		cronJobLister: cronJobLister,
		host:          host,
		httpClient:    &http.Client{},
	}
	go c.Run(1, stop)
	return c
}
func (c *Controller) Run(workers int, stopCh <-chan struct{}) {
	defer func() {
		if err := recover(); err != nil {
		}
	}()
	defer c.queue.ShutDown()

	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}

	go wait.Until(c.cronJobLister.CornJobWatcher, time.Second*10, stopCh)
	<-stopCh
}

func (c *Controller) worker() {
	for c.processNextWorkItem() {

	}
}

func (c *Controller) processNextWorkItem() bool {
	// 阻塞
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	requeueAfter, err := c.sync(key.(string))
	switch {
	case err != nil:
		// DefaultControllerRateLimiter 目前使用的是默认速率限制器 指数级的
		c.queue.AddRateLimited(key)
	case requeueAfter != nil:
		// 执行成功 获取到下一次调度时间
		c.queue.Forget(key)
		c.queue.AddAfter(key, *requeueAfter)
	}
	return true
}

// CornJobWatcher 监听数据库变更  model hook监听不了数据库变更
func (s *service) CornJobWatcher() {
	var activityKeys []string
	db := s.mysql.Model(&model.Activity{})
	if err := db.Where("type = ?", model.Job).Pluck("activity_key", &activityKeys).Error; err != nil {
		return
	}
	// 再获取一遍是保证缓存更新了
	activities, err := s.GetActivities(context.Background(), activityKeys)
	if err != nil {
		return
	}
	defer func() {
		s.old = activities
	}()
	// 第一次运行
	if len(s.old) == 0 {
		for _, activity := range activities {
			s.controller.queue.Add(activity.ActivityKey)
		}
	} else {
		activitiesMap := make(map[string]*model.Activity, len(activities))
		for _, activity := range activities {
			activitiesMap[activity.ActivityKey] = activity
		}

		for i, activity := range s.old {
			curr, exist := activitiesMap[activity.ActivityKey]
			// 原来有现在没有
			if !exist {
				s.controller.queue.Forget(activity.ActivityKey)
				continue
			}

			if reflect.DeepEqual(curr, s.old[i]) {
				delete(activitiesMap, activity.ActivityKey)
				// 原来和现在一样
				continue
			}
			// 原来和现在不一样
			s.controller.updateCronJob(s.old[i], curr)
			delete(activitiesMap, activity.ActivityKey)
		}
		for _, activity := range activitiesMap {
			// 原来没有现在有
			s.controller.queue.Add(activity.ActivityKey)
		}
	}

	return
}

func (c *Controller) sync(key string) (*time.Duration, error) {
	// 留ctx是为了tracing用
	background := context.Background()
	activity, err := c.cronJobLister.GetActivity(background, key)
	if err != nil {
		if IsGetActivityRecordNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	if activity.Finished {
		return nil, nil
	}
	duration, err := c.syncCronJob(activity)
	if err != nil {
		return nil, err
	}
	if duration != nil {
		return duration, nil
	}
	return nil, nil
}

func (c *Controller) syncCronJob(activity *model.Activity) (*time.Duration, error) {
	var (
		scheduleTime *time.Time
		err          error
	)
	sched, err := cron.ParseStandard(activity.Schedule)
	if err != nil {
		c.cronJobLister.SetLastScheduleResp(activity, err.Error())
		return nil, nil
	}
	now := time.Now()

	if activity.MT == true {
		scheduleTime = &now
		defer func() {
			c.cronJobLister.SetFinishMT(activity)
		}()
	} else {
		scheduleTime, err = getNextScheduleTime(activity, now, sched)
		if err != nil {
			c.cronJobLister.SetLastScheduleResp(activity, err.Error())
			return nil, nil
		}
		if scheduleTime == nil {
			// 下次调度时间为空 重新入队排列
			t := nextScheduledTimeDuration(sched, now)
			return t, nil
		}
	}
	c.cronJobLister.SetLastScheduleTime(activity, *scheduleTime)
	if err != nil {
		c.cronJobLister.SetLastScheduleResp(activity, err.Error())
		return nil, nil
	}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, c.host+activity.ActivityKey, bytes.NewReader(activity.Body))
	if err != nil {
		// todo 现在失败是跳过  如果返回err就是指数级重试
		c.cronJobLister.SetLastScheduleResp(activity, err.Error())
		return nil, nil
	}

	resp, err := c.httpClient.Do(request)
	if err != nil {
		c.cronJobLister.SetLastScheduleResp(activity, err.Error())
		return nil, nil
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		c.cronJobLister.SetLastScheduleResp(activity, err.Error())
		return nil, nil
	}
	if body != nil {
		c.cronJobLister.SetLastScheduleResp(activity, string(body))
		t := nextScheduledTimeDuration(sched, now)
		return t, nil
	}
	c.cronJobLister.SetLastScheduleResp(activity, "目标服务器什么都没返回")
	return nil, nil
}

func (s *service) SetLastScheduleTime(activity *model.Activity, lastScheduleTime time.Time) {
	updates := s.mysql.Model(activity).Updates(map[string]interface{}{
		"last_schedule_time": lastScheduleTime.Unix(),
		"last_schedule_resp": "",
	})
	err := updates.Error
	if err != nil {
	}
	if updates.RowsAffected == 0 {
	}
	return
}

func (s *service) SetFinishMT(activity *model.Activity) {
	updates := s.mysql.Model(activity).Updates(map[string]interface{}{
		"mt":       false,
		"finished": true,
	})
	err := updates.Error
	if err != nil {
	}
	if updates.RowsAffected == 0 {
	}
	return
}

func (s *service) SetLastScheduleResp(activity *model.Activity, resp string) {
	updates := s.mysql.Model(activity).Updates(map[string]interface{}{
		"last_schedule_resp": resp,
	})
	err := updates.Error
	if err != nil {
	}
	if updates.RowsAffected == 0 {
	}
	return
}

func nextScheduledTimeDuration(sched cron.Schedule, now time.Time) *time.Duration {
	t := sched.Next(now).Add(nextScheduleDelta).Sub(now)
	return &t
}

func getNextScheduleTime(activity *model.Activity, now time.Time, schedule cron.Schedule) (*time.Time, error) {

	earliestTime := time.Unix(int64(activity.StartTime), 0)

	if activity.LastScheduleTime != 0 {
		earliestTime = time.Unix(int64(activity.LastScheduleTime), 0)
	}

	if earliestTime.After(now) {
		return nil, nil
	}

	t, numberOfMissedSchedules, err := getMostRecentScheduleTime(earliestTime, now, schedule)
	if numberOfMissedSchedules > 100 {
		// 如果是以分钟计算的cronjob 服务器宕机时间超过2小时那么没有必要再追回了 或者是系统时间不对 导致需要追的次数太大直接返回
	}
	return t, err
}

// 获取最近的调度时间
func getMostRecentScheduleTime(earliestTime, now time.Time, schedule cron.Schedule) (*time.Time, int64, error) {
	t1 := schedule.Next(earliestTime)
	t2 := schedule.Next(t1)

	if now.Before(t1) {
		return nil, 0, nil
	}
	if now.Before(t2) {
		return &t1, 1, nil
	}

	// It is possible for cron.ParseStandard("59 23 31 2 *") to return an invalid schedule
	// seconds - 59, minute - 23, hour - 31 (?!)  dom - 2, and dow is optional, clearly 31 is invalid
	// In this case the timeBetweenTwoSchedules will be 0, and we error out the invalid schedule
	timeBetweenTwoSchedules := int64(t2.Sub(t1).Round(time.Second).Seconds())
	if timeBetweenTwoSchedules < 1 {
		return nil, 0, fmt.Errorf("time difference between two schedules less than 1 second")
	}
	timeElapsed := int64(now.Sub(t1).Seconds())
	numberOfMissedSchedules := (timeElapsed / timeBetweenTwoSchedules) + 1
	t := time.Unix(t1.Unix()+((numberOfMissedSchedules-1)*timeBetweenTwoSchedules), 0).UTC()
	return &t, numberOfMissedSchedules, nil
}

func (c *Controller) updateCronJob(old, curr *model.Activity) {
	switch {
	case old.LastScheduleTime != curr.LastScheduleTime:
		return
	case old.LastScheduleResp != curr.LastScheduleResp:
		return
	case old.Schedule != curr.Schedule:
		sched, err := cron.ParseStandard(curr.Schedule)
		if err != nil {
			return
		}
		now := time.Now()
		t := nextScheduledTimeDuration(sched, now)
		c.queue.AddAfter(curr.ActivityKey, *t)
		return
	case old.Finished == false && curr.Finished == true:
		c.queue.Forget(old.ActivityKey)
		return
	default:
		c.queue.Add(curr.ActivityKey)
	}
}

func (s *service) GetActivity(ctx context.Context, key string) (result *model.Activity, err error) {
	objDest := &keysToActivityArr{}
	if err = s.objCache(ctx, []string{key}, objDest); err != nil {
		return
	}
	if len(objDest.dest) > 0 {
		return objDest.dest[0], nil
	}

	return nil, fmt.Errorf("key: %v", key)
}
