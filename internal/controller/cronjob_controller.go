/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/robfig/cron"
	kbatch "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/reference"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	batchv1 "will.kubebuilder.io/cronjob/api/v1"
)

// CronJobReconciler reconciles a CronJob object
type CronJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Clock
}

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (_ realClock) Now() time.Time {
	return time.Now()
}

//+kubebuilder:rbac:groups=batch.will.kubebuilder.io,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch.will.kubebuilder.io,resources=cronjobs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=batch.will.kubebuilder.io,resources=cronjobs/finalizers,verbs=update
//+kubebuidler:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuidler:rbac:groups=batch,resources=jobs/status,verbs=get

var scheduleTimeAnnotation = "batch.will.kubebuilder.io/scheduled-at"

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the CronJob object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.15.0/pkg/reconcile
func (r *CronJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 1. Load cronjob
	var cronjob batchv1.CronJob
	if err := r.Get(ctx, req.NamespacedName, &cronjob); err != nil {
		log.Error(err, "unable to fetch CronJob")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 2. Update cronjob status
	var childJobs kbatch.JobList
	if err := r.List(ctx, &childJobs, client.InNamespace(req.Namespace), client.MatchingFields{jobOwnerKey: req.Name}); err != nil {
		log.Error(err, "unable to list Job")
		return ctrl.Result{}, err
	}

	var activeJobs []*kbatch.Job
	var failedJobs []*kbatch.Job
	var completeJobs []*kbatch.Job
	var mostRecentTime *time.Time

	isJobFinished := func(job *kbatch.Job) (bool, kbatch.JobConditionType) {
		for _, c := range job.Status.Conditions {
			if c.Type == kbatch.JobComplete || c.Type == kbatch.JobFailed && c.Status == "True" {
				return true, c.Type
			}
		}
		return false, ""
	}

	getScheduledTimeForJob := func(job *kbatch.Job) (*time.Time, error) {
		timeRaw := job.Annotations[scheduleTimeAnnotation]
		if len(timeRaw) == 0 {
			return nil, nil
		}
		timeParsed, err := time.Parse(time.RFC3339, timeRaw)
		if err != nil {
			return nil, err
		}
		return &timeParsed, nil
	}

	for _, job := range childJobs.Items {
		_, finishedType := isJobFinished(&job)
		switch finishedType {
		case "": // active
			activeJobs = append(activeJobs, &job)
		case kbatch.JobFailed:
			failedJobs = append(failedJobs, &job)
		case kbatch.JobComplete:
			completeJobs = append(completeJobs, &job)
		}
		scheduledTime, err := getScheduledTimeForJob(&job)
		if err != nil {
			log.Error(err, "unable to parse schedule time for job", "job", &job)
			continue
		}
		if scheduledTime != nil {
			if mostRecentTime == nil {
				mostRecentTime = scheduledTime
			} else if mostRecentTime.Before(*scheduledTime) {
				mostRecentTime = scheduledTime
			}
		}

	}
	if mostRecentTime != nil {
		cronjob.Status.LastScheduleTime = &metav1.Time{Time: *mostRecentTime}
	} else {
		cronjob.Status.LastScheduleTime = nil
	}

	cronjob.Status.Active = nil
	for _, activeJob := range activeJobs {
		jobRef, err := reference.GetReference(r.Scheme, activeJob)
		if err != nil {
			log.Error(err, "unable to make reference to active job", "job", activeJob)
			continue
		}
		cronjob.Status.Active = append(cronjob.Status.Active, *jobRef)
	}

	log.V(1).Info("job count", "active", len(activeJobs), "failed", len(failedJobs), "complete", len(completeJobs))

	if err := r.Status().Update(ctx, &cronjob); err != nil {
		log.Error(err, "unable to update CronJob status")
		return ctrl.Result{}, err
	}

	// 3. Clean up old jobs
	if cronjob.Spec.FailedJobsHistoryLimit != nil {
		sort.Slice(failedJobs, func(i, j int) bool {
			if failedJobs[i].Status.StartTime == nil {
				return failedJobs[j].Status.StartTime != nil
			}
			return failedJobs[i].Status.StartTime.Before(failedJobs[j].Status.StartTime)
		})
		for i := 0; i < len(failedJobs)-int(*cronjob.Spec.FailedJobsHistoryLimit); i++ {
			if err := r.Delete(ctx, failedJobs[i], client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				log.Error(err, "unable to delete old failed job", "job", failedJobs[i])
			} else {
				log.V(1).Info("deleted old failed job", "job", failedJobs[i])
			}
		}
	}
	if cronjob.Spec.SuccessfulJobsHistoryLimit != nil {
		sort.Slice(completeJobs, func(i, j int) bool {
			if completeJobs[i].Status.StartTime == nil {
				return completeJobs[j].Status.StartTime != nil
			}
			return completeJobs[i].Status.StartTime.Before(completeJobs[j].Status.StartTime)
		})
		for i := 0; i < len(completeJobs)-int(*cronjob.Spec.SuccessfulJobsHistoryLimit); i++ {
			if err := r.Delete(ctx, completeJobs[i], client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				log.Error(err, "unable to delete old complete job", "job", completeJobs[i])
			} else {
				log.V(1).Info("deleted old complete job", "job", completeJobs[i])
			}
		}
	}

	// 4. Check if suspended
	if cronjob.Spec.Suspend != nil && *cronjob.Spec.Suspend {
		log.V(1).Info("cronjob suspended, skipping")
		return ctrl.Result{}, nil
	}

	// 5. Get the next scheduled time
	getNextScheduledTime := func(cronjob *batchv1.CronJob, now time.Time) (lastMissedRun, nextRun time.Time, err error) {
		schedule, err := cron.ParseStandard(cronjob.Spec.Schedule)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("unparseable schedule %q: %v", cronjob.Spec.Schedule, err)
		}

		// earliestTime is the startpoint from which to search for the next schedule time.
		var earliestTime time.Time
		if cronjob.Status.LastScheduleTime != nil {
			earliestTime = cronjob.Status.LastScheduleTime.Time
		} else {
			earliestTime = cronjob.ObjectMeta.CreationTimestamp.Time
		}
		if cronjob.Spec.StartingDeadlineSeconds != nil {
			startingDeadline := now.Add(-time.Duration(*cronjob.Spec.StartingDeadlineSeconds) * time.Second)
			if earliestTime.Before(startingDeadline) {
				earliestTime = startingDeadline
			}
		}
		if earliestTime.After(now) {
			return time.Time{}, schedule.Next(now), nil
		}

		starts := 0
		for t := schedule.Next(earliestTime); !t.After(now); t = schedule.Next(t) {
			lastMissedRun = t
			starts++
			if starts > 100 {
				return time.Time{}, time.Time{}, fmt.Errorf("too many missed start times (> 100). Set or decrease .spec.startingDeadlineSeconds or check clock skew")
			}
			earliestTime = t
		}
		return lastMissedRun, schedule.Next(now), nil
	}

	missedRun, nextRun, err := getNextScheduledTime(&cronjob, r.Now())
	if err != nil {
		log.Error(err, "unable to figure out cronjob schedule")
		return ctrl.Result{}, nil
	}

	scheduledResult := ctrl.Result{RequeueAfter: nextRun.Sub(r.Now())}
	log = log.WithValues("now", r.Now(), "nextRun", nextRun)

	// 6. Check if missed
	if missedRun.IsZero() {
		log.V(1).Info("no missed run")
		return scheduledResult, nil
	}
	log = log.WithValues("currentRun", missedRun)

	// if missedRun is older than startingDeadline, skip it
	toolate := false
	if cronjob.Spec.StartingDeadlineSeconds != nil {
		toolate = missedRun.Add(time.Duration(*cronjob.Spec.StartingDeadlineSeconds) * time.Second).Before(r.Now())
	}
	if toolate {
		log.V(1).Info("missed run was too long ago, skipping")
		return scheduledResult, nil
	}

	// 7. Check concurrency policy
	if cronjob.Spec.ConcurrencyPolicy == batchv1.ForbidConcurrent && len(activeJobs) > 0 {
		log.V(1).Info("concurrency policy is Forbid, skipping")
		return scheduledResult, nil
	}
	if cronjob.Spec.ConcurrencyPolicy == batchv1.ReplaceConcurrent {
		for _, activeJob := range activeJobs {
			if err := r.Delete(ctx, activeJob, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				log.Error(err, "unable to delete old active job", "job", activeJob)
				return ctrl.Result{}, err
			} else {
				log.V(1).Info("deleted old active job", "job", activeJob)
			}
		}
	}

	// 8. Create new job
	constructNewJob := func(cronJob *batchv1.CronJob, scheduledTime time.Time) (*kbatch.Job, error) {
		name := fmt.Sprintf("%s-%d", cronJob.Name, scheduledTime.Unix())
		job := &kbatch.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   cronJob.Namespace,
				Annotations: make(map[string]string),
				Labels:      make(map[string]string),
			},
			Spec: *cronJob.Spec.JobTemplate.Spec.DeepCopy(),
		}

		for k, v := range cronJob.Spec.JobTemplate.Annotations {
			job.Annotations[k] = v
		}
		job.Annotations[scheduleTimeAnnotation] = scheduledTime.Format(time.RFC3339)
		for k, v := range cronJob.Spec.JobTemplate.Labels {
			job.Labels[k] = v
		}
		if err := ctrl.SetControllerReference(cronJob, job, r.Scheme); err != nil {
			return nil, err
		}

		return job, nil
	}
	job, err := constructNewJob(&cronjob, missedRun)
	if err != nil {
		log.Error(err, "unable to construct new job")
		return scheduledResult, nil
	}
	if err := r.Create(ctx, job); err != nil {
		log.Error(err, "unable to create new job")
		return ctrl.Result{}, err
	}

	log.V(1).Info("created new job", "job", job)

	// 8. Requeue
	return scheduledResult, nil
}

var jobOwnerKey = ".metadata.controller"
var apiGVStr = batchv1.GroupVersion.String()

// SetupWithManager sets up the controller with the Manager.
func (r *CronJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Clock == nil {
		r.Clock = realClock{}
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &kbatch.Job{}, jobOwnerKey, func(o client.Object) []string {
		owner := metav1.GetControllerOf(o.(*kbatch.Job))
		if owner == nil {
			return nil
		}
		if owner.APIVersion != apiGVStr || owner.Kind != "CronJob" {
			return nil
		}
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.CronJob{}).
		Complete(r)
}
