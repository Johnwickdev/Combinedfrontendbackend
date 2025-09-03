package com.autotrade.trading;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.trader.backend.service.QuantAnalysisService;
import com.trader.backend.service.Tick;

@Component
public class StrategyEngine {
    private static final Logger log = LoggerFactory.getLogger(StrategyEngine.class);

    private final QuantAnalysisService quantAnalysisService;
    private final OrderRouter orderRouter;
    private final RiskManager riskManager;

    private volatile String currentInstrument;
    private final AtomicBoolean inPosition = new AtomicBoolean(false);
    private final AtomicReference<OrderIntent.Side> positionSide = new AtomicReference<>(null);

    public StrategyEngine(QuantAnalysisService quantAnalysisService, OrderRouter orderRouter, RiskManager riskManager) {
        this.quantAnalysisService = quantAnalysisService;
        this.orderRouter = orderRouter;
        this.riskManager = riskManager;
    }

    public void onTick(Tick tick) {
        if (tick == null || currentInstrument == null || !currentInstrument.equals(tick.instrumentKey())) {
            return;
        }

        QuantAnalysisService.QuantAnalysisResult result = quantAnalysisService.computeAnalysis(tick);
        QuantAnalysisService.Signal signal = result.signal();

        if (!inPosition.get()) {
            if (signal == QuantAnalysisService.Signal.ENTRY_LONG || signal == QuantAnalysisService.Signal.ENTRY_SHORT) {
                if (riskManager.canEnter(1)) {
                    OrderIntent.Side side = signal == QuantAnalysisService.Signal.ENTRY_LONG ? OrderIntent.Side.BUY : OrderIntent.Side.SELL;
                    OrderIntent intent = new OrderIntent(signal.name(), tick.instrumentKey(), side,
                            OrderIntent.OrderType.MARKET, OrderIntent.Mode.PAPER, 1, tick.ltp());
                    String orderId = orderRouter.placeOrder(intent);
                    if (orderId != null) {
                        inPosition.set(true);
                        positionSide.set(side);
                        riskManager.recordEntry();
                    }
                }
            }
        } else if (signal == QuantAnalysisService.Signal.EXIT) {
            OrderIntent.Side side = positionSide.get() == OrderIntent.Side.BUY ? OrderIntent.Side.SELL : OrderIntent.Side.BUY;
            OrderIntent intent = new OrderIntent(signal.name(), tick.instrumentKey(), side,
                    OrderIntent.OrderType.MARKET, OrderIntent.Mode.PAPER, 1, tick.ltp());
            String orderId = orderRouter.placeOrder(intent);
            if (orderId != null) {
                inPosition.set(false);
                positionSide.set(null);
                riskManager.recordExit(0.0);
            }
        }
    }

    public void setInstrument(String instrumentKey) {
        this.currentInstrument = instrumentKey;
        inPosition.set(false);
        positionSide.set(null);
    }
}
