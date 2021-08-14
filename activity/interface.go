package activity

import (
	"context"
	"go-linter-84/model"
)

type IActivity interface {
	getActivities(ctx context.Context, keys []string) (activities []*model.Activity, err error)
	GetActivities(ctx context.Context, ids []string) (activities []*model.Activity, err error)
	Acting(ctx context.Context, key string) bool
	CronJobLister
}

type CreateActivity interface {
	CreateActivity(ctx context.Context, activity *model.Activity) (result *model.Activity, err error)
}
