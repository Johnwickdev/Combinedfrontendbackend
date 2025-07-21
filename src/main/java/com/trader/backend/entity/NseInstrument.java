package com.trader.backend.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/*

@Data
@Document(collection = "nse_instruments")
public class NseInstrument {

    @Id
    private String instrument_key;

    private boolean weekly;
    private String segment;
    private String name;
    private String exchange;
    private long expiry;
    */
/*@Field("instrument_type")
    private String instrumentType;
*//*

    @Field("instrument_type")
    private String instrumentType;

    public String getInstrument_key() {
        return instrument_key;
    }

    public void setInstrument_key(String instrument_key) {
        this.instrument_key = instrument_key;
    }

    public boolean isWeekly() {
        return weekly;
    }

    public void setWeekly(boolean weekly) {
        this.weekly = weekly;
    }

    public String getSegment() {
        return segment;
    }

    public void setSegment(String segment) {
        this.segment = segment;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public long getExpiry() {
        return expiry;
    }

    public void setExpiry(long expiry) {
        this.expiry = expiry;
    }

    public String getInstrumentType() {
        return instrumentType;
    }

    public void setInstrumentType(String instrumentType) {
        this.instrumentType = instrumentType;
    }

    public double getStrikePrice() {
        return strikePrice;
    }

    public void setStrikePrice(double strikePrice) {
        this.strikePrice = strikePrice;
    }

    public String getAsset_symbol() {
        return asset_symbol;
    }

    public void setAsset_symbol(String asset_symbol) {
        this.asset_symbol = asset_symbol;
    }

    public String getUnderlying_symbol() {
        return underlying_symbol;
    }

    public void setUnderlying_symbol(String underlying_symbol) {
        this.underlying_symbol = underlying_symbol;
    }

    public int getLot_size() {
        return lot_size;
    }

    public void setLot_size(int lot_size) {
        this.lot_size = lot_size;
    }

    public double getFreeze_quantity() {
        return freeze_quantity;
    }

    public void setFreeze_quantity(double freeze_quantity) {
        this.freeze_quantity = freeze_quantity;
    }

    public String getExchange_token() {
        return exchange_token;
    }

    public void setExchange_token(String exchange_token) {
        this.exchange_token = exchange_token;
    }

    public int getMinimum_lot() {
        return minimum_lot;
    }

    public void setMinimum_lot(int minimum_lot) {
        this.minimum_lot = minimum_lot;
    }

    public String getAsset_key() {
        return asset_key;
    }

    public void setAsset_key(String asset_key) {
        this.asset_key = asset_key;
    }

    public String getUnderlying_key() {
        return underlying_key;
    }

    public void setUnderlying_key(String underlying_key) {
        this.underlying_key = underlying_key;
    }

    public double getTick_size() {
        return tick_size;
    }

    public void setTick_size(double tick_size) {
        this.tick_size = tick_size;
    }

    public String getAsset_type() {
        return asset_type;
    }

    public void setAsset_type(String asset_type) {
        this.asset_type = asset_type;
    }

    public String getUnderlying_type() {
        return underlying_type;
    }

    public void setUnderlying_type(String underlying_type) {
        this.underlying_type = underlying_type;
    }

    public String getTrading_symbol() {
        return trading_symbol;
    }

    public void setTrading_symbol(String trading_symbol) {
        this.trading_symbol = trading_symbol;
    }

    public double getQty_multiplier() {
        return qty_multiplier;
    }

    public void setQty_multiplier(double qty_multiplier) {
        this.qty_multiplier = qty_multiplier;
    }

    @Field("strike_price")
    private double strikePrice;

    private String asset_symbol;
    private String underlying_symbol;
    private int lot_size;
    private double freeze_quantity;
    private String exchange_token;
    private int minimum_lot;
    private String asset_key;
    private String underlying_key;
    private double tick_size;
    private String asset_type;
    private String underlying_type;
    private String trading_symbol;
   // private double strike_price;
    private double qty_multiplier;
}
*/


import com.fasterxml.jackson.annotation.JsonProperty;


@Data
@Document(collection = "nse_instruments")
public class NseInstrument {

    @Id
    @JsonProperty("instrument_key")
    private String instrument_key;

    @JsonProperty("weekly")
    private boolean weekly;

    @JsonProperty("segment")
    private String segment;

    @JsonProperty("name")
    private String name;

    @JsonProperty("exchange")
    private String exchange;

    @JsonProperty("expiry")
    private long expiry;

    @JsonProperty("instrument_type")
    private String instrumentType;

    @JsonProperty("strike_price")
    private double strikePrice;

    @JsonProperty("asset_symbol")
    private String asset_symbol;

    @JsonProperty("underlying_symbol")
    private String underlying_symbol;

    @JsonProperty("lot_size")
    private int lot_size;

    @JsonProperty("freeze_quantity")
    private double freeze_quantity;

    @JsonProperty("exchange_token")
    private String exchange_token;

    @JsonProperty("minimum_lot")
    private int minimum_lot;

    @JsonProperty("asset_key")
    private String asset_key;

    @JsonProperty("underlying_key")
    private String underlying_key;

    @JsonProperty("tick_size")
    private double tick_size;

    @JsonProperty("asset_type")
    private String asset_type;

    @JsonProperty("underlying_type")
    private String underlying_type;

    @JsonProperty("trading_symbol")
    private String trading_symbol;

    @JsonProperty("qty_multiplier")
    private double qty_multiplier;
}
