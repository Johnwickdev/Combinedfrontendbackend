package com.trader.backend.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

@JsonIgnoreProperties(ignoreUnknown = true)
@Document(collection = "nse_instruments")
public class NseInstrument {

    @Id
    @JsonProperty("instrument_key")
    @Field("instrument_key")
    private String instrumentKey;

    @JsonProperty("weekly")
    @Field("weekly")
    private boolean weekly;

    @JsonProperty("segment")
    @Field("segment")
    private String segment;

    @JsonProperty("name")
    @Field("name")
    private String name;

    @JsonProperty("exchange")
    @Field("exchange")
    private String exchange;

    @JsonProperty("expiry")
    @Field("expiry")
    private long expiry;

    @JsonProperty("instrument_type")
    @Field("instrument_type")
    private String instrumentType;

    @JsonProperty("strike_price")
    @Field("strike_price")
    private Integer strikePrice;

    @JsonProperty("asset_symbol")
    @Field("asset_symbol")
    private String assetSymbol;

    @JsonProperty("underlying_symbol")
    @Field("underlying_symbol")
    private String underlyingSymbol;

    @JsonProperty("lot_size")
    @Field("lot_size")
    private int lotSize;

    @JsonProperty("freeze_quantity")
    @Field("freeze_quantity")
    private double freezeQuantity;

    @JsonProperty("exchange_token")
    @Field("exchange_token")
    private String exchangeToken;

    @JsonProperty("minimum_lot")
    @Field("minimum_lot")
    private int minimumLot;

    @JsonProperty("asset_key")
    @Field("asset_key")
    private String assetKey;

    @JsonProperty("underlying_key")
    @Field("underlying_key")
    private String underlyingKey;

    @JsonProperty("tick_size")
    @Field("tick_size")
    private double tickSize;

    @JsonProperty("asset_type")
    @Field("asset_type")
    private String assetType;

    @JsonProperty("underlying_type")
    @Field("underlying_type")
    private String underlyingType;

    @JsonProperty("trading_symbol")
    @Field("trading_symbol")
    private String tradingSymbol;

    @JsonProperty("qty_multiplier")
    @Field("qty_multiplier")
    private double qtyMultiplier;

    public String getInstrumentKey() { return instrumentKey; }
    public void setInstrumentKey(String instrumentKey) { this.instrumentKey = instrumentKey; }

    public boolean isWeekly() { return weekly; }
    public void setWeekly(boolean weekly) { this.weekly = weekly; }

    public String getSegment() { return segment; }
    public void setSegment(String segment) { this.segment = segment; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getExchange() { return exchange; }
    public void setExchange(String exchange) { this.exchange = exchange; }

    public long getExpiry() { return expiry; }
    public void setExpiry(long expiry) { this.expiry = expiry; }

    public String getInstrumentType() { return instrumentType; }
    public void setInstrumentType(String instrumentType) { this.instrumentType = instrumentType; }

    public Integer getStrikePrice() { return strikePrice; }
    public void setStrikePrice(Integer strikePrice) { this.strikePrice = strikePrice; }

    public String getAssetSymbol() { return assetSymbol; }
    public void setAssetSymbol(String assetSymbol) { this.assetSymbol = assetSymbol; }

    public String getUnderlyingSymbol() { return underlyingSymbol; }
    public void setUnderlyingSymbol(String underlyingSymbol) { this.underlyingSymbol = underlyingSymbol; }

    public int getLotSize() { return lotSize; }
    public void setLotSize(int lotSize) { this.lotSize = lotSize; }

    public double getFreezeQuantity() { return freezeQuantity; }
    public void setFreezeQuantity(double freezeQuantity) { this.freezeQuantity = freezeQuantity; }

    public String getExchangeToken() { return exchangeToken; }
    public void setExchangeToken(String exchangeToken) { this.exchangeToken = exchangeToken; }

    public int getMinimumLot() { return minimumLot; }
    public void setMinimumLot(int minimumLot) { this.minimumLot = minimumLot; }

    public String getAssetKey() { return assetKey; }
    public void setAssetKey(String assetKey) { this.assetKey = assetKey; }

    public String getUnderlyingKey() { return underlyingKey; }
    public void setUnderlyingKey(String underlyingKey) { this.underlyingKey = underlyingKey; }

    public double getTickSize() { return tickSize; }
    public void setTickSize(double tickSize) { this.tickSize = tickSize; }

    public String getAssetType() { return assetType; }
    public void setAssetType(String assetType) { this.assetType = assetType; }

    public String getUnderlyingType() { return underlyingType; }
    public void setUnderlyingType(String underlyingType) { this.underlyingType = underlyingType; }

    public String getTradingSymbol() { return tradingSymbol; }
    public void setTradingSymbol(String tradingSymbol) { this.tradingSymbol = tradingSymbol; }

    public double getQtyMultiplier() { return qtyMultiplier; }
    public void setQtyMultiplier(double qtyMultiplier) { this.qtyMultiplier = qtyMultiplier; }
}

