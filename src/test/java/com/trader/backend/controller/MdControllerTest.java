package com.trader.backend.controller;

import com.trader.backend.dto.OptionType;
import com.trader.backend.dto.TradeRow;
import com.trader.backend.service.CandleService;
import com.trader.backend.service.InfluxTickService;
import com.trader.backend.service.LiveFeedService;
import com.trader.backend.service.NseInstrumentService;
import com.trader.backend.service.SelectionService;
import com.trader.backend.service.TradeHistoryService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@WebFluxTest(MdController.class)
class MdControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockBean private CandleService candleService;
    @MockBean private LiveFeedService liveFeedService;
    @MockBean private NseInstrumentService nseInstrumentService;
    @MockBean private SelectionService selectionService;
    @MockBean private InfluxTickService influxTickService;
    @MockBean private TradeHistoryService tradeHistoryService;

    @Test
    void sectorTradesReturnsArray() {
        TradeRow row = new TradeRow(
                Instant.parse("2025-08-27T10:21:30Z"),
                "NSE_FO|64103",
                OptionType.CE,
                24800,
                182.5,
                -0.12,
                5,
                120000,
                "NSE_FO|64103-1693125690");
        Mockito.when(tradeHistoryService.fetchRecentOptionTrades(50, "both"))
                .thenReturn(Optional.of(new TradeHistoryService.Result(List.of(row), "live")));

        webTestClient.get().uri("/md/sector-trades")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Source", "live")
                .expectBodyList(TradeRow.class)
                .hasSize(1)
                .contains(row);
    }

    @Test
    void sectorTradesNoDataReturnsEmptyArray() {
        Mockito.when(tradeHistoryService.fetchRecentOptionTrades(50, "both"))
                .thenReturn(Optional.empty());

        webTestClient.get().uri("/md/sector-trades")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Source", "none")
                .expectBodyList(TradeRow.class)
                .hasSize(0);
    }
}

