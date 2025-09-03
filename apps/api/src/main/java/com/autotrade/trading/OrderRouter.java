package com.autotrade.trading;

public interface OrderRouter {
    String placeOrder(OrderIntent intent);
    boolean cancelOrder(String orderId);
}
