// Copyright 2016 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	info "github.com/google/cadvisor/info/v1"
	"github.com/google/cadvisor/storage"
	"github.com/google/cadvisor/utils/container"

	"github.com/apache/pulsar-client-go/pulsar"
	"k8s.io/klog"
)

func init() {
	storage.RegisterStorageDriver("pulsar", new)
	// pulsar.Logger = log.New(os.Stderr, "[pulsar]", log.LstdFlags)
}

var (
	broker = flag.String("storage_driver_pulsar_broker", "localhost:6650", "pulsar broker csv")
	topic  = flag.String("storage_driver_pulsar_topic", "stats", "pulsar topic")
)

type pulsarStorage struct {
	producer pulsar.Producer
	topic       string
	machineName string
}

type detailSpec struct {
	Timestamp       time.Time            `json:"timestamp"`
	MachineName     string               `json:"machine_name,omitempty"`
	ContainerName   string               `json:"container_Name,omitempty"`
	ContainerID     string               `json:"container_Id,omitempty"`
	ContainerLabels map[string]string    `json:"container_labels,omitempty"`
	ContainerStats  *info.ContainerStats `json:"container_stats,omitempty"`
}

func (driver *pulsarStorage) infoToDetailSpec(cInfo *info.ContainerInfo, stats *info.ContainerStats) *detailSpec {
	timestamp := time.Now()
	containerID := cInfo.ContainerReference.Id
	containerLabels := cInfo.Spec.Labels
	containerName := container.GetPreferredName(cInfo.ContainerReference)

	detail := &detailSpec{
		Timestamp:       timestamp,
		MachineName:     driver.machineName,
		ContainerName:   containerName,
		ContainerID:     containerID,
		ContainerLabels: containerLabels,
		ContainerStats:  stats,
	}
	return detail
}

func (driver *pulsarStorage) AddStats(cInfo *info.ContainerInfo, stats *info.ContainerStats) error {
	detail := driver.infoToDetailSpec(cInfo, stats)
	b, err := json.Marshal(detail)

	if _, err := n.producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(b),
		}); err != nil {
			return fmt.Errorf("FAILED to send pulsar message: %s", err)
		}

	return err
}

func (self *pulsarStorage) Close() error {
	return self.producer.Close()
}

func new() (storage.StorageDriver, error) {
	machineName, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	return newStorage(machineName)
}

func newStorage(machineName string) (storage.StorageDriver, error) {


	pulsarClient, _ := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://" + broker,
	})

	producer, _ := pulsarClient.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})


	// config := pulsarStorage.NewConfig()
	// config.Producer.RequiredAcks = kafka.WaitForAll
	// brokerList := strings.Split(*brokers, ",")
	// klog.V(4).Infof("Kafka brokers:%q", *brokers)
	// producer, err := kafka.NewAsyncProducer(brokerList, config)
	// if err != nil {
	// 	return nil, err
	// }
	ret := &pulsarStorage{
		producer:    producer,
		topic:       *topic,
		machineName: machineName,
	}
	return ret, nil
}
