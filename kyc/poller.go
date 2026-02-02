package kyc

import (
	"context"
	"sync"
	"time"

	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
)

type Poller struct {
	engine   *WorkflowEngine
	ds       KYCDataSource
	interval time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func NewPoller(engine *WorkflowEngine, ds KYCDataSource, interval time.Duration) *Poller {
	return &Poller{
		engine:   engine,
		ds:       ds,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

func (p *Poller) Start() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()

		logrus.Infof("KYC Poller started with interval: %v", p.interval)

		p.pollPendingSteps()

		for {
			select {
			case <-ticker.C:
				p.pollPendingSteps()
			case <-p.stopCh:
				logrus.Info("KYC Poller stopping...")
				return
			}
		}
	}()
}

func (p *Poller) Stop() {
	close(p.stopCh)
	p.wg.Wait()
	logrus.Info("KYC Poller stopped")
}

func (p *Poller) pollPendingSteps() {
	ctx := context.Background()

	steps, err := p.ds.GetKYCStepsByStatus(model.StepStatusSubmitted)
	if err != nil {
		logrus.Errorf("Poller: failed to fetch submitted steps: %v", err)
		return
	}

	if len(steps) == 0 {
		logrus.Debug("Poller: no pending steps to check")
		return
	}

	logrus.Infof("Poller: checking %d pending steps", len(steps))

	for _, step := range steps {
		p.checkStepStatus(ctx, &step)
	}
}

func (p *Poller) checkStepStatus(ctx context.Context, step *model.KYCStep) {
	workflow, err := p.ds.GetKYCWorkflow(step.WorkflowID)
	if err != nil {
		logrus.Errorf("Poller: failed to get workflow %s: %v", step.WorkflowID, err)
		return
	}

	provider, exists := p.engine.providers[workflow.ProviderID]
	if !exists {
		logrus.Errorf("Poller: provider %s not found for workflow %s", workflow.ProviderID, workflow.WorkflowID)
		return
	}

	if step.ProviderRef == "" {
		logrus.Warnf("Poller: step %s has no provider reference, skipping", step.StepID)
		return
	}

	result, err := provider.CheckStatus(ctx, step.ProviderRef)
	if err != nil {
		logrus.Errorf("Poller: CheckStatus failed for step %s (ref: %s): %v", step.StepID, step.ProviderRef, err)
		return
	}

	logrus.Infof("Poller: step %s status from provider: %s", step.StepID, result.Status)

	if result.Status == StatusVerified {
		step.Status = model.StepStatusPassed
		step.Result = result.RawData
	} else if result.Status == StatusFailed {
		step.Status = model.StepStatusFailed
		step.Result = result.RawData
	} else if result.Status == StatusReview {
		logrus.Infof("Poller: step %s needs manual review", step.StepID)
		return
	} else {
		logrus.Debugf("Poller: step %s still pending", step.StepID)
		return
	}

	if err := p.ds.UpdateKYCStep(step); err != nil {
		logrus.Errorf("Poller: failed to update step %s: %v", step.StepID, err)
		return
	}

	p.engine.evaluateWorkflow(ctx, workflow)
}
