package com.autotrade.trading;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingOrderRouter implements OrderRouter {
    private static final Logger log = LoggerFactory.getLogger(LoggingOrderRouter.class);

    @Override
    public String placeOrder(OrderIntent intent) {
        String id = UUID.randomUUID().toString();
        log.info("Placing order signalId={} instrument={} side={} qty={} type={} price={} mode={}",
                intent.getSignalId(), intent.getInstrumentKey(), intent.getSide(),
                intent.getQuantity(), intent.getOrderType(), intent.getPrice(), intent.getMode());
        return id;
    }

    @Override
    public boolean cancelOrder(String orderId) {
        log.info("Cancelling order {}", orderId);
        return true;
    }
}
