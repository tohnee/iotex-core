// Copyright (c) 2019 IoTeX Foundation
// This is an alpha (internal) release and is not suitable for production. This source code is provided 'as is' and no
// warranties are given as to title or non-infringement, merchantability or fitness for purpose and, to the extent
// permitted by law, all liability for your use of the code is disclaimed. This source code is governed by Apache
// License 2.0 that can be found in the LICENSE file.

package itx

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/iotexproject/go-fsm"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/iotexproject/iotex-core/consensus"
	"github.com/iotexproject/iotex-core/consensus/scheme"
	"github.com/iotexproject/iotex-core/consensus/scheme/rolldpos"
	"github.com/iotexproject/iotex-core/dispatcher"
	"github.com/iotexproject/iotex-core/pkg/log"
	"github.com/iotexproject/iotex-core/pkg/version"
)

// TODO: HeartbeatHandler opens encapsulation of a few structs to inspect the internal status, we need to find a better
// approach to do so in the future

var heartbeatMtc = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "iotex_heartbeat_status",
		Help: "Node heartbeat status.",
	},
	[]string{"status_type", "source"},
)

var versionMtc = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "iotex_version_status",
		Help: "Node software version status.",
	},
	[]string{"type", "value"},
)

func init() {
	prometheus.MustRegister(heartbeatMtc)
	prometheus.MustRegister(versionMtc)
}

// HeartbeatHandler is the handler to periodically log the system key metrics
type HeartbeatHandler struct {
	s *Server
}

// NewHeartbeatHandler instantiates a HeartbeatHandler instance
func NewHeartbeatHandler(s *Server) *HeartbeatHandler {
	return &HeartbeatHandler{s: s}
}

// Log executes the logging logic
func (h *HeartbeatHandler) Log() {
	// Network metrics
	p2pAgent := h.s.P2PAgent()

	// Dispatcher metrics
	dp, ok := h.s.Dispatcher().(*dispatcher.IotxDispatcher)
	if !ok {
		log.L().Error("dispatcher is not the instance of IotxDispatcher")
		return
	}
	numDPEvts := len(*dp.EventChan())
	dpEvtsAudit, err := json.Marshal(dp.EventAudit())
	if err != nil {
		log.L().Error("error when serializing the dispatcher event audit map.", zap.Error(err))
		return
	}

	ctx := context.Background()
	peers, err := p2pAgent.Neighbors(ctx)
	if err != nil {
		log.L().Debug("error when get neighbors.", zap.Error(err))
		peers = nil
	}
	numPeers := len(peers)
	log.L().Info("Node status.",
		zap.Int("numPeers", numPeers),
		zap.Int("pendingDispatcherEvents", numDPEvts),
		zap.String("pendingDispatcherEventsAudit", string(dpEvtsAudit)))

	heartbeatMtc.WithLabelValues("numPeers", "node").Set(float64(numPeers))
	heartbeatMtc.WithLabelValues("pendingDispatcherEvents", "node").Set(float64(numDPEvts))
	// chain service
	for _, c := range h.s.chainservices {
		// Consensus metrics
		cs, ok := c.Consensus().(*consensus.IotxConsensus)
		if !ok {
			log.L().Info("consensus is not the instance of IotxConsensus.")
			return
		}
		rolldpos, ok := cs.Scheme().(*rolldpos.RollDPoS)
		numPendingEvts := 0
		consensusEpoch := uint64(0)
		consensusHeight := uint64(0)

		var consensusMetrics scheme.ConsensusMetrics
		var state fsm.State
		if ok {
			numPendingEvts = rolldpos.NumPendingEvts()
			state = rolldpos.CurrentState()

			// RollDpos Concensus Metrics
			consensusMetrics, err = rolldpos.Metrics()
			if err != nil {
				log.L().Error("failed to read consensus metrics", zap.Error(err))
				return
			}
			consensusEpoch = consensusMetrics.LatestEpoch
			consensusHeight = consensusMetrics.LatestHeight
		} else {
			log.L().Debug("scheme is not the instance of RollDPoS")
		}

		// Block metrics
		height := c.Blockchain().TipHeight()

		actPoolSize := c.ActionPool().GetSize()
		actPoolCapacity := c.ActionPool().GetCapacity()
		targetHeight := c.BlockSync().TargetHeight()

		log.L().Info("chain service status",
			zap.Int("rolldposEvents", numPendingEvts),
			zap.String("fsmState", string(state)),
			zap.Uint64("blockchainHeight", height),
			zap.Uint64("actpoolSize", actPoolSize),
			zap.Uint64("actpoolCapacity", actPoolCapacity),
			zap.Uint32("chainID", c.ChainID()),
			zap.Uint64("targetHeight", targetHeight),
			zap.Uint64("concensusEpoch", consensusEpoch),
			zap.Uint64("consensusHeight", consensusHeight),
		)

		chainIDStr := strconv.FormatUint(uint64(c.ChainID()), 10)
		heartbeatMtc.WithLabelValues("consensusEpoch", chainIDStr).Set(float64(consensusHeight))
		heartbeatMtc.WithLabelValues("consensusRound", chainIDStr).Set(float64(consensusEpoch))
		heartbeatMtc.WithLabelValues("pendingRolldposEvents", chainIDStr).Set(float64(numPendingEvts))
		heartbeatMtc.WithLabelValues("blockchainHeight", chainIDStr).Set(float64(height))
		heartbeatMtc.WithLabelValues("actpoolSize", chainIDStr).Set(float64(actPoolSize))
		heartbeatMtc.WithLabelValues("actpoolCapacity", chainIDStr).Set(float64(actPoolCapacity))
		heartbeatMtc.WithLabelValues("targetHeight", chainIDStr).Set(float64(targetHeight))
		heartbeatMtc.WithLabelValues("packageVersion", version.PackageVersion).Set(1)
		heartbeatMtc.WithLabelValues("packageCommitID", version.PackageCommitID).Set(1)
		heartbeatMtc.WithLabelValues("goVersion", version.GoVersion).Set(1)
	}

}
