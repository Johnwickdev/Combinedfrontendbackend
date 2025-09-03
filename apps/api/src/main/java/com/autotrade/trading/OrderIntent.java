package com.autotrade.trading;

public class OrderIntent {
    public enum Side { BUY, SELL }
    public enum OrderType { MARKET, LIMIT }
    public enum Mode { PAPER, LIVE }

    private final String signalId;
    private final String instrumentKey;
    private final Side side;
    private final OrderType orderType;
    private final Mode mode;
    private final int quantity;
    private final double price;

    public OrderIntent(String signalId, String instrumentKey, Side side,
                       OrderType orderType, Mode mode, int quantity, double price) {
        this.signalId = signalId;
        this.instrumentKey = instrumentKey;
        this.side = side;
        this.orderType = orderType;
        this.mode = mode;
        this.quantity = quantity;
        this.price = price;
    }

    public String getSignalId() { return signalId; }
    public String getInstrumentKey() { return instrumentKey; }
    public Side getSide() { return side; }
    public OrderType getOrderType() { return orderType; }
    public Mode getMode() { return mode; }
    public int getQuantity() { return quantity; }
    public double getPrice() { return price; }
}
