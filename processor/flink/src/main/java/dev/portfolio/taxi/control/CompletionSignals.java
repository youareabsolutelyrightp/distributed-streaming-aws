package dev.portfolio.taxi.control;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

public final class CompletionSignals {
    public static final OutputTag<RunControlRecord> QUERY1 = new OutputTag<>(
            "query1-complete-v1", TypeInformation.of(RunControlRecord.class));
    public static final OutputTag<RunControlRecord> QUERY2 = new OutputTag<>(
            "query2-complete-v1", TypeInformation.of(RunControlRecord.class));

    private CompletionSignals() {}
}
