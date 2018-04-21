/*
Copyright 2017 The Kubernetes Authors.

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

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Azure-Samples/azure-sdk-for-go-samples/helpers"
	"github.com/Azure-Samples/azure-sdk-for-go-samples/iam"
	"github.com/Azure-Samples/azure-sdk-for-go-samples/resources"
	"github.com/Azure/azure-sdk-for-go/services/redis/mgmt/2017-10-01/redis"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/to"
	"github.com/golang/glog"
	appsv1beta2 "k8s.io/api/apps/v1beta2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	redisv1alpha1 "k8s.io/redis-controller/pkg/apis/rediscontroller/v1alpha1"
	clientset "k8s.io/redis-controller/pkg/client/clientset/versioned"
	redischeme "k8s.io/redis-controller/pkg/client/clientset/versioned/scheme"
	informers "k8s.io/redis-controller/pkg/client/informers/externalversions"
	listers "k8s.io/redis-controller/pkg/client/listers/rediscontroller/v1alpha1"
)

const controllerAgentName = "redis-controller"

const (
	// SuccessSynced is used as part of the Event 'reason' when a Redis is synced
	SuccessSynced = "Synced"
	// ErrResourceExists is used as part of the Event 'reason' when a Redis fails
	// to sync due to a Deployment of the same name already existing.
	ErrResourceExists = "ErrResourceExists"

	// MessageResourceExists is the message used for Events when a resource
	// fails to sync due to a Deployment already existing
	MessageResourceExists = "Resource %q already exists and is not managed by Redis"
	// MessageResourceSynced is the message used for an Event fired when a Redis
	// is synced successfully
	MessageResourceSynced = "Redis synced successfully"
)

// Controller is the controller implementation for Redis resources
type Controller struct {
	// kubeclientset is a standard kubernetes clientset
	kubeclientset kubernetes.Interface
	// redisclientset is a clientset for our own API group
	redisclientset clientset.Interface

	redisLister listers.RedisLister
	redisSynced cache.InformerSynced

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	workqueue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder
}

// NewController returns a new redis controller
func NewController(
	kubeclientset kubernetes.Interface,
	redisclientset clientset.Interface,
	kubeInformerFactory kubeinformers.SharedInformerFactory,
	redisInformerFactory informers.SharedInformerFactory) *Controller {

	// obtain references to shared index informers for the Deployment and Redis
	// types.
	redisInformer := redisInformerFactory.Rediscontroller().V1alpha1().Redises()

	// Create event broadcaster
	// Add redis-controller types to the default Kubernetes Scheme so Events can be
	// logged for redis-controller types.
	redischeme.AddToScheme(scheme.Scheme)
	glog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	controller := &Controller{
		kubeclientset:  kubeclientset,
		redisclientset: redisclientset,
		redisLister:    redisInformer.Lister(),
		redisSynced:    redisInformer.Informer().HasSynced,
		workqueue:      workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "redis"),
		recorder:       recorder,
	}

	glog.Info("Setting up event handlers")
	// Set up an event handler for when Redis resources change
	redisInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueRedis,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueRedis(new)
		},
	})

	return controller
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.workqueue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	glog.Info("Starting Redis controller")

	// Wait for the caches to be synced before starting workers
	glog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.redisSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	glog.Info("Starting workers")
	// Launch two workers to process Redis resources
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	glog.Info("Started workers")
	<-stopCh
	glog.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		defer c.workqueue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.workqueue.Forget(obj)
			runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// Redis resource to be synced.
		if _, err := c.redisDeploy(key); err != nil {
			return fmt.Errorf("error syncing '%s': %s", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.workqueue.Forget(obj)
		glog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		runtime.HandleError(err)
		return true
	}

	return true
}

func (c *Controller) redisDeploy(key string) (server redis.ResourceType, err error) {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return server, nil
	}

	// Get the redis resource with this namespace/name
	r, err := c.redisLister.Redises(namespace).Get(name)
	if err != nil {
		// The Redis resource may no longer exist, in which case we stop
		// processing.
		if errors.IsNotFound(err) {
			runtime.HandleError(fmt.Errorf("redis '%s' in work queue no longer exists", key))
			return server, nil
		}

		return server, err
	}

	resourceGroupName := os.Getenv("AZ_RESOURCE_GROUP_NAME")
	name = strings.ToLower(name)
	helpers.PrintAndLog(fmt.Sprintf("%v server create request for %v namespace", name, namespace))
	ctx := context.Background()
	//	defer resources.Cleanup(ctx)
	_, err = resources.CreateGroup(ctx, resourceGroupName)
	if err != nil {
		helpers.PrintAndLog(err.Error())
	}

	SkuName := redis.SkuName(r.Spec.SkuName)
	SkuFamily := redis.SkuFamily(r.Spec.SkuFamily)

	redisClient := getRedisClient()
	future, err := redisClient.Create(
		ctx,
		helpers.ResourceGroupName(),
		name,
		redis.CreateParameters{
			Location: to.StringPtr("northeurope"),
			CreateProperties: &redis.CreateProperties{
				EnableNonSslPort: to.BoolPtr(true),
				Sku: &redis.Sku{Name: SkuName, Family: SkuFamily,
					Capacity: to.Int32Ptr(r.Spec.SkuCapacity)},
			},
		})

	if err != nil {
		//      return err
		return server, fmt.Errorf("cannot create redis server: %v", err)
	}

	err = future.WaitForCompletion(ctx, redisClient.Client)
	if err != nil {
		return server, fmt.Errorf("cannot get the redis cache create or update future response: %v", err) //              return fmt.Errorf("cannot get the sql server create or update future response: %v", err)        }
		//      return err
		return future.Result(redisClient)
	}

	//	_, err = CreateServer(ctx, name)
	//	if err != nil {
	//		helpers.PrintAndLog(fmt.Sprintf("cannot create redis server: %v", err))
	//	}

	helpers.PrintAndLog(fmt.Sprintf("redis server  %v is created", name))
	return server, err
}

func (c *Controller) updateRediStatus(redis *redisv1alpha1.Redis, deployment *appsv1beta2.Deployment) error {
	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use DeepCopy() to make a deep copy of original object and modify this copy
	// Or create a copy manually for better performance
	redisCopy := redis.DeepCopy()
	redisCopy.Status.AvailableReplicas = deployment.Status.AvailableReplicas
	// Until #38113 is merged, we must use Update instead of UpdateStatus to
	// update the Status block of the Redis resource. UpdateStatus will not
	// allow changes to the Spec of the resource, which is ideal for ensuring
	// nothing other than resource status has been updated.
	_, err := c.redisclientset.RediscontrollerV1alpha1().Redises(redis.Namespace).Update(redisCopy)
	return err
}

// enqueueRedis takes a Redis resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than Redis.
func (c *Controller) enqueueRedis(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}
	c.workqueue.AddRateLimited(key)
}

// handleObject will take any resource implementing metav1.Object and attempt
// to find the Redis resource that 'owns' it. It does this by looking at the
// objects metadata.ownerReferences field for an appropriate OwnerReference.
// It then enqueues that Redis resource to be processed. If the object does not
// have an appropriate OwnerReference, it will simply be skipped.
func (c *Controller) handleObject(obj interface{}) {
	var object metav1.Object
	var ok bool
	if object, ok = obj.(metav1.Object); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			runtime.HandleError(fmt.Errorf("error decoding object, invalid type"))
			return
		}
		object, ok = tombstone.Obj.(metav1.Object)
		if !ok {
			runtime.HandleError(fmt.Errorf("error decoding object tombstone, invalid type"))
			return
		}
		glog.V(4).Infof("Recovered deleted object '%s' from tombstone", object.GetName())
	}
	glog.V(4).Infof("Processing object: %s", object.GetName())
	if ownerRef := metav1.GetControllerOf(object); ownerRef != nil {
		// If this object is not owned by a Redis, we should not do anything more
		// with it.
		if ownerRef.Kind != "Redis" {
			return
		}

		redis, err := c.redisLister.Redises(object.GetNamespace()).Get(ownerRef.Name)
		if err != nil {
			glog.V(4).Infof("ignoring orphaned object '%s' of redis '%s'", object.GetSelfLink(), ownerRef.Name)
			return
		}

		c.enqueueRedis(redis)
		return
	}
}

func getRedisClient() redis.Client {
	token, _ := iam.GetResourceManagementToken(iam.AuthGrantType())
	redisClient := redis.NewClient(helpers.SubscriptionID())
	redisClient.Authorizer = autorest.NewBearerAuthorizer(token)
	redisClient.AddToUserAgent("pidahsamples")
	return redisClient
}
