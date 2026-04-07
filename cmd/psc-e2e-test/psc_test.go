/*
Copyright 2021 The Kubernetes Authors.

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
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/cloud-provider-gcp/providers/gce"
	"k8s.io/ingress-gce/pkg/e2e"
	"k8s.io/utils/ptr"
)

func TestPSCLifecycle(t *testing.T) {
	t.Parallel()

	Framework.RunWithSandbox("PSC Lifecycle", t, func(t *testing.T, s *e2e.Sandbox) {
		svcName := fmt.Sprintf("ilb-service-e2e-%x", s.RandInt)
		saName := fmt.Sprintf("service-attachment-e2e-%x", s.RandInt)

		// PSC requires subnet to have purpose PRIVATE_SERVICE_CONNECT
		err := e2e.CreateSubnet(s, e2e.PSCSubnetName, e2e.PSCSubnetPurpose)
		defer e2e.DeleteSubnet(s, e2e.PSCSubnetName)
		if err != nil {
			// Subnet Creation could fail because of a leaked subnet from a previous run.
			// Instead of failing the test, attempt to reuse subnet.
			t.Logf("error creating subnet %s: %q", e2e.PSCSubnetName, err)
		} else {
			t.Logf("created subnet for PSC")
		}

		l4ILBAnnotation := map[string]string{gce.ServiceAnnotationLoadBalancerType: "Internal"}
		if _, err := e2e.EnsureEchoService(s, svcName, l4ILBAnnotation, v1.ServiceTypeLoadBalancer, 1); err != nil {
			t.Fatalf("error ensuring echo service %s/%s: %q", s.Namespace, svcName, err)
		}
		t.Logf("ensured echo service %s/%s", s.Namespace, svcName)

		if _, err := e2e.EnsureServiceAttachment(s, saName, svcName, e2e.PSCSubnetName); err != nil {
			t.Fatalf("error creating service attachment cr %s/%s: %q", s.Namespace, saName, err)
		}

		gceSAURL, err := e2e.WaitForServiceAttachment(s, saName)
		if err != nil {
			t.Fatalf("failed waiting for service attachment: %q", err)
		}

		if err := e2e.DeleteServiceAttachment(s, saName); err != nil {
			t.Fatalf("failed deleting service attachment %s/%s: %q", s.Namespace, saName, err)
		}

		if err := e2e.WaitForServiceAttachmentDeletion(s, saName, gceSAURL); err != nil {
			t.Fatalf("failed while waiting for service attachment deletion")
		}
	})
}

func TestPSCReconcileConnectionsExplicitSync(t *testing.T) {
	t.Parallel()

	Framework.RunWithSandbox("PSC Reconcile Connections Explicit Sync", t, func(t *testing.T, s *e2e.Sandbox) {
		svcName := fmt.Sprintf("ilb-service-e2e-%x", s.RandInt)
		saName := fmt.Sprintf("service-attachment-e2e-%x", s.RandInt)

		// PSC requires subnet to have purpose PRIVATE_SERVICE_CONNECT
		err := e2e.CreateSubnet(s, e2e.PSCSubnetName, e2e.PSCSubnetPurpose)
		defer e2e.DeleteSubnet(s, e2e.PSCSubnetName)
		if err != nil {
			// Subnet Creation could fail because of a leaked subnet from a previous run.
			// Instead of failing the test, attempt to reuse subnet.
			t.Logf("error creating subnet %s: %q", e2e.PSCSubnetName, err)
		} else {
			t.Logf("created subnet for PSC")
		}

		l4ILBAnnotation := map[string]string{gce.ServiceAnnotationLoadBalancerType: "Internal"}
		if _, err := e2e.EnsureEchoService(s, svcName, l4ILBAnnotation, v1.ServiceTypeLoadBalancer, 1); err != nil {
			t.Fatalf("error ensuring echo service %s/%s: %q", s.Namespace, svcName, err)
		}
		t.Logf("ensured echo service %s/%s", s.Namespace, svcName)

		if _, err := e2e.EnsureServiceAttachmentWithSpec(s, saName, svcName, e2e.PSCSubnetName, ptr.To(true)); err != nil {
			t.Fatalf("error creating service attachment cr %s/%s: %q", s.Namespace, saName, err)
		}

		gceSAURL, err := e2e.WaitForServiceAttachment(s, saName)
		if err != nil {
			t.Fatalf("failed waiting for service attachment: %q", err)
		}

		gceSA, err := e2e.GetGCEServiceAttachmentFromURL(s, gceSAURL)
		if err != nil {
			t.Fatalf("failed getting GCE service attachment from URL %q: %q", gceSAURL, err)
		}
		if !gceSA.ReconcileConnections {
			t.Fatalf("expected ReconcileConnections to be true, got false")
		}

		if _, err := e2e.EnsureServiceAttachmentWithSpec(s, saName, svcName, e2e.PSCSubnetName, ptr.To(false)); err != nil {
			t.Fatalf("error updating service attachment cr %s/%s: %q", s.Namespace, saName, err)
		}

		_, err = e2e.WaitForServiceAttachment(s, saName)
		if err != nil {
			t.Fatalf("failed waiting for service attachment update: %q", err)
		}

		var updated bool
		for i := 0; i < 30; i++ {
			gceSA, err = e2e.GetGCEServiceAttachmentFromURL(s, gceSAURL)
			if err == nil && !gceSA.ReconcileConnections {
				updated = true
				break
			}
			time.Sleep(5 * time.Second)
		}
		if !updated {
			t.Fatalf("expected ReconcileConnections to be false, got true")
		}

		if err := e2e.DeleteServiceAttachment(s, saName); err != nil {
			t.Fatalf("failed deleting service attachment %s/%s: %q", s.Namespace, saName, err)
		}

		if err := e2e.WaitForServiceAttachmentDeletion(s, saName, gceSAURL); err != nil {
			t.Fatalf("failed while waiting for service attachment deletion")
		}
	})
}

func TestPSCUnsyncedStateProtection(t *testing.T) {
	t.Parallel()

	Framework.RunWithSandbox("PSC Unsynced State Protection", t, func(t *testing.T, s *e2e.Sandbox) {
		svcName := fmt.Sprintf("ilb-service-e2e-%x", s.RandInt)
		saName := fmt.Sprintf("service-attachment-e2e-%x", s.RandInt)

		// 1. Setup Phase
		err := e2e.CreateSubnet(s, e2e.PSCSubnetName, e2e.PSCSubnetPurpose)
		defer e2e.DeleteSubnet(s, e2e.PSCSubnetName)
		if err != nil {
			t.Logf("error creating subnet %s: %q", e2e.PSCSubnetName, err)
		} else {
			t.Logf("created subnet for PSC")
		}

		l4ILBAnnotation := map[string]string{gce.ServiceAnnotationLoadBalancerType: "Internal"}
		if _, err := e2e.EnsureEchoService(s, svcName, l4ILBAnnotation, v1.ServiceTypeLoadBalancer, 1); err != nil {
			t.Fatalf("error ensuring echo service %s/%s: %q", s.Namespace, svcName, err)
		}
		t.Logf("ensured echo service %s/%s", s.Namespace, svcName)

		if _, err := e2e.EnsureServiceAttachmentWithSpec(s, saName, svcName, e2e.PSCSubnetName, nil); err != nil {
			t.Fatalf("error creating service attachment cr %s/%s: %q", s.Namespace, saName, err)
		}

		gceSAURL, err := e2e.WaitForServiceAttachment(s, saName)
		if err != nil {
			t.Fatalf("failed waiting for service attachment: %q", err)
		}

		// 2. Simulate Manual Drift
		gceSA, err := e2e.GetGCEServiceAttachmentFromURL(s, gceSAURL)
		if err != nil {
			t.Fatalf("failed getting GCE service attachment from URL %q: %q", gceSAURL, err)
		}
		gceSA.ReconcileConnections = true
		if err := e2e.PatchGCEServiceAttachment(s, gceSAURL, gceSA); err != nil {
			t.Fatalf("failed patching GCE service attachment: %q", err)
		}

		// 3. Trigger Reconciliation
		saCR, err := e2e.GetServiceAttachmentCR(s, saName)
		if err != nil {
			t.Fatalf("failed getting service attachment cr: %q", err)
		}
		if saCR.Annotations == nil {
			saCR.Annotations = make(map[string]string)
		}
		saCR.Annotations["dummy-update"] = "true"
		if _, err := Framework.SAClient.Update(saCR); err != nil {
			t.Fatalf("failed updating service attachment cr: %q", err)
		}

		var unsynced bool
		for i := 0; i < 30; i++ {
			saCR, err = e2e.GetServiceAttachmentCR(s, saName)
			if err != nil {
				t.Fatalf("failed getting service attachment cr: %q", err)
			}
			if saCR.Annotations != nil && saCR.Annotations["networking.gke.io/unsynced-field"] == "ReconcileConnections" {
				unsynced = true
				break
			}
			time.Sleep(5 * time.Second)
		}
		if !unsynced {
			t.Fatalf("expected networking.gke.io/unsynced-field annotation to contain ReconcileConnections")
		}

		// 4. Verify Unsync Protection
		gceSA, err = e2e.GetGCEServiceAttachmentFromURL(s, gceSAURL)
		if err != nil {
			t.Fatalf("failed getting GCE service attachment from URL %q: %q", gceSAURL, err)
		}
		if !gceSA.ReconcileConnections {
			t.Fatalf("expected ReconcileConnections to still be true, got false")
		}

		// 5. Verify Resolution
		if _, err := e2e.EnsureServiceAttachmentWithSpec(s, saName, svcName, e2e.PSCSubnetName, ptr.To(false)); err != nil {
			t.Fatalf("error updating service attachment cr %s/%s: %q", s.Namespace, saName, err)
		}

		var resolved bool
		for i := 0; i < 30; i++ {
			gceSA, err = e2e.GetGCEServiceAttachmentFromURL(s, gceSAURL)
			if err == nil && !gceSA.ReconcileConnections {
				resolved = true
				break
			}
			time.Sleep(5 * time.Second)
		}
		if !resolved {
			t.Fatalf("expected ReconcileConnections to become false, got true")
		}

		saCR, err = e2e.GetServiceAttachmentCR(s, saName)
		if err != nil {
			t.Fatalf("failed getting service attachment cr: %q", err)
		}
		if saCR.Annotations != nil && saCR.Annotations["networking.gke.io/unsynced-field"] == "ReconcileConnections" {
			t.Fatalf("expected networking.gke.io/unsynced-field annotation to be removed or not contain ReconcileConnections")
		}

		// 6. Cleanup
		if err := e2e.DeleteServiceAttachment(s, saName); err != nil {
			t.Fatalf("failed deleting service attachment %s/%s: %q", s.Namespace, saName, err)
		}

		if err := e2e.WaitForServiceAttachmentDeletion(s, saName, gceSAURL); err != nil {
			t.Fatalf("failed while waiting for service attachment deletion")
		}
	})
}
