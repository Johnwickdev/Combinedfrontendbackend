package com.trader.backend.controller;

import com.trader.backend.entity.Selection;
import com.trader.backend.repository.SelectionRepository;
import com.trader.backend.service.Tick;
import com.trader.backend.service.TickStore;
import com.trader.backend.service.UpstoxFeedV3Client;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@RestController
@RequiredArgsConstructor
public class LtpController {
    private final TickStore tickStore;
    private final UpstoxFeedV3Client feed;
    private final SelectionRepository selectionRepository;

    @GetMapping("/ltp/nifty-fut")
    public Map<String, Object> niftyFut() {
        String key = feed.symbols().stream().findFirst().orElse(null);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("key", key);
        Optional<Tick> t = key == null ? Optional.empty() : tickStore.get(key);
        if (t.isPresent()) {
            m.put("ltp", t.get().ltp());
            m.put("ts", t.get().ts());
            m.put("source", "live");
        } else {
            m.put("source", "influx");
        }
        return m;
    }

    @GetMapping("/ltp/options-atm")
    public Map<String, Object> optionsAtm() {
        Selection sel = selectionRepository.findAll(Sort.by(Sort.Direction.DESC, "createdAt")).stream().findFirst().orElse(null);
        Map<String, Object> resp = new LinkedHashMap<>();
        if (sel == null) {
            return resp;
        }
        resp.put("baseKey", sel.getBaseKey());
        resp.put("baseLtp", sel.getBaseLtp());
        resp.put("atmStrike", sel.getAtmStrike());
        resp.put("ce", buildTickMap(sel.getCeKey()));
        resp.put("pe", buildTickMap(sel.getPeKey()));
        return resp;
    }

    private Map<String, Object> buildTickMap(String key) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("key", key);
        Optional<Tick> t = tickStore.get(key);
        if (t.isPresent()) {
            m.put("ltp", t.get().ltp());
            m.put("ts", t.get().ts());
            m.put("source", "live");
        } else {
            m.put("source", "influx");
        }
        return m;
    }
}
