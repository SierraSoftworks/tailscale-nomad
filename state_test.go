package main

import (
	"context"
	"testing"
)

func TestRegistrationInScope(t *testing.T) {
	tests := []struct {
		name  string
		reg   serviceRegistration
		scope string
		want  bool
	}{
		{name: "node local", reg: serviceRegistration{NodeID: "node-1", Datacenter: "dc-1", Address: "10.0.0.1"}, scope: "node", want: true},
		{name: "node remote", reg: serviceRegistration{NodeID: "node-2", Datacenter: "dc-1", Address: "10.0.0.2"}, scope: "node", want: false},
		{name: "datacenter local", reg: serviceRegistration{NodeID: "node-2", Datacenter: "dc-1", Address: "10.0.0.2"}, scope: "datacenter", want: true},
		{name: "datacenter remote", reg: serviceRegistration{NodeID: "node-2", Datacenter: "dc-2", Address: "10.0.0.2"}, scope: "datacenter", want: false},
		{name: "global", reg: serviceRegistration{NodeID: "node-2", Datacenter: "dc-2", Address: "10.0.0.2"}, scope: "global", want: true},
		{name: "loopback local", reg: serviceRegistration{NodeID: "node-1", Datacenter: "dc-2", Address: "127.1.2.3"}, scope: "global", want: true},
		{name: "loopback remote", reg: serviceRegistration{NodeID: "node-2", Datacenter: "dc-1", Address: "127.0.0.1"}, scope: "global", want: false},
		{name: "ipv6 loopback remote", reg: serviceRegistration{NodeID: "node-2", Datacenter: "dc-1", Address: "::1"}, scope: "global", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got := registrationScopeRank(tt.reg, tt.scope, "node-1", "dc-1")
			if got != tt.want {
				t.Fatalf("registrationScopeRank() eligible = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDesiredFromStatePrefersLocalBackend(t *testing.T) {
	tags := []string{"tailscale.enable=true", "tailscale.scope=global"}
	state := serviceState{
		registrationKey("default", "local"):   {ID: "local", ServiceName: "web", Namespace: "default", NodeID: "node-1", Datacenter: "dc-1", Tags: tags, Address: "10.0.0.1", Port: 8001, CreateIndex: 1},
		registrationKey("default", "same-dc"): {ID: "same-dc", ServiceName: "web", Namespace: "default", NodeID: "node-2", Datacenter: "dc-1", Tags: tags, Address: "10.0.0.2", Port: 8002, CreateIndex: 2},
		registrationKey("default", "remote"):  {ID: "remote", ServiceName: "web", Namespace: "default", NodeID: "node-3", Datacenter: "dc-2", Tags: tags, Address: "10.0.0.3", Port: 8003, CreateIndex: 3},
	}
	desired := desiredFromState(context.Background(), state, "node-1", "dc-1", "tailscale", defaultProxyConfig(256))
	if len(desired) != 1 || desired[0].Backend != "10.0.0.1:8001" {
		t.Fatalf("desired = %+v, want node-local backend", desired)
	}
}

func TestDesiredFromStatePrefersDatacenterBackendForGlobalScope(t *testing.T) {
	tags := []string{"tailscale.enable=true", "tailscale.scope=global"}
	state := serviceState{
		registrationKey("default", "same-dc"): {ID: "same-dc", ServiceName: "web", Namespace: "default", NodeID: "node-2", Datacenter: "dc-1", Tags: tags, Address: "10.0.0.2", Port: 8002, CreateIndex: 1},
		registrationKey("default", "remote"):  {ID: "remote", ServiceName: "web", Namespace: "default", NodeID: "node-3", Datacenter: "dc-2", Tags: tags, Address: "10.0.0.3", Port: 8003, CreateIndex: 2},
	}
	desired := desiredFromState(context.Background(), state, "node-1", "dc-1", "tailscale", defaultProxyConfig(256))
	if len(desired) != 1 || desired[0].Backend != "10.0.0.2:8002" {
		t.Fatalf("desired = %+v, want datacenter-local backend", desired)
	}
}

func TestDesiredFromStateSelectsNewestRegistrationInScope(t *testing.T) {
	tags := []string{"tailscale.enable=true", "tailscale.scope=datacenter"}
	state := serviceState{
		registrationKey("default", "old"):    {ID: "old", ServiceName: "web", Namespace: "default", NodeID: "node-4", Datacenter: "dc-1", Tags: tags, Address: "10.0.0.1", Port: 8001, CreateIndex: 1},
		registrationKey("default", "new"):    {ID: "new", ServiceName: "web", Namespace: "default", NodeID: "node-2", Datacenter: "dc-1", Tags: tags, Address: "10.0.0.2", Port: 8002, CreateIndex: 2},
		registrationKey("default", "remote"): {ID: "remote", ServiceName: "web", Namespace: "default", NodeID: "node-3", Datacenter: "dc-2", Tags: tags, Address: "10.0.0.3", Port: 8003, CreateIndex: 3},
	}
	desired := desiredFromState(context.Background(), state, "node-1", "dc-1", "tailscale", defaultProxyConfig(256))
	if len(desired) != 1 || desired[0].Backend != "10.0.0.2:8002" {
		t.Fatalf("desired = %+v, want newest dc-1 backend", desired)
	}
}

func TestServiceStateAppliesEventsIncrementally(t *testing.T) {
	state := serviceState{}
	upsert := serviceEventBatch{Index: 10, Events: []serviceEvent{{
		Type: "ServiceRegistration", Key: "reg-1", Namespace: "default", Index: 10,
		Payload: struct{ Service serviceRegistration }{Service: serviceRegistration{
			ID: "reg-1", ServiceName: "web", Namespace: "default", Tags: []string{"tailscale.enable=true"}, ModifyIndex: 10,
		}},
	}}}
	if !state.apply(upsert, "tailscale") || len(state) != 1 {
		t.Fatalf("upsert did not populate state: %+v", state)
	}

	stale := upsert
	stale.Events[0].Payload.Service.ModifyIndex = 9
	stale.Events[0].Payload.Service.Address = "stale"
	if !state.apply(stale, "tailscale") || state[registrationKey("default", "reg-1")].Address == "stale" {
		t.Fatal("stale upsert replaced current registration")
	}

	deleteBatch := serviceEventBatch{Index: 11, Events: []serviceEvent{{
		Type: "ServiceDeregistration", Key: "reg-1", Namespace: "default", Index: 11,
	}}}
	if !state.apply(deleteBatch, "tailscale") || len(state) != 0 {
		t.Fatalf("delete did not remove registration: %+v", state)
	}
}

func TestServiceStateRejectsUnknownEvents(t *testing.T) {
	state := serviceState{}
	if state.apply(serviceEventBatch{Events: []serviceEvent{{Type: "Unexpected", Key: "x", Namespace: "default"}}}, "tailscale") {
		t.Fatal("unknown event should request a full repair")
	}
}

func TestServiceStateDropsDisabledRegistration(t *testing.T) {
	key := registrationKey("default", "reg-1")
	state := serviceState{key: {ID: "reg-1", ServiceName: "web", Namespace: "default", Tags: []string{"tailscale.enable=true"}}}
	batch := serviceEventBatch{Events: []serviceEvent{{
		Type: "ServiceRegistration", Key: "reg-1", Namespace: "default",
		Payload: struct{ Service serviceRegistration }{Service: serviceRegistration{
			ID: "reg-1", ServiceName: "web", Namespace: "default", Tags: nil, ModifyIndex: 2,
		}},
	}}}
	if !state.apply(batch, "tailscale") || len(state) != 0 {
		t.Fatalf("disabled registration remains cached: %+v", state)
	}
}
