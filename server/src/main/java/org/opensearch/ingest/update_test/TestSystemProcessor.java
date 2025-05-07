/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.update_test;


import org.opensearch.ingest.AbstractBatchingSystemProcessor;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestDocumentWrapper;
import org.opensearch.ingest.Processor;
import org.opensearch.ingest.ValueSource;
import org.opensearch.script.ScriptService;
import org.opensearch.script.TemplateScript;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Processor that adds new fields with their corresponding values. If the field is already present, its value
 * will be replaced with the provided one.
 */
public final class TestSystemProcessor extends AbstractBatchingSystemProcessor {
    public static final String TYPE = "set";

    private final boolean overrideEnabled;
    private final TemplateScript.Factory field;
    private final ValueSource value;
    private final boolean ignoreEmptyValue;

    TestSystemProcessor(String tag, String description, TemplateScript.Factory field, ValueSource value) {
        this(tag, description, field, value, true, false);
    }

    TestSystemProcessor(
        String tag,
        String description,
        TemplateScript.Factory field,
        ValueSource value,
        boolean overrideEnabled,
        boolean ignoreEmptyValue
    ) {
        super(tag, description, 10);
        this.overrideEnabled = overrideEnabled;
        this.field = field;
        this.value = value;
        this.ignoreEmptyValue = ignoreEmptyValue;
    }

    public boolean isOverrideEnabled() {
        return overrideEnabled;
    }

    public TemplateScript.Factory getField() {
        return field;
    }

    public ValueSource getValue() {
        return value;
    }

    public boolean isIgnoreEmptyValue() {
        return ignoreEmptyValue;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        System.out.println("Execute triggered in test process for " + document);
        document.appendFieldValue(field, value, true);
        return document;
    }

    @Override
    public void subBatchExecute(List<IngestDocumentWrapper> ingestDocumentWrappers, Consumer<List<IngestDocumentWrapper>> handler) {
        System.out.println("Sub batch execute triggered in test process for " + ingestDocumentWrappers);
        if (ingestDocumentWrappers == null || ingestDocumentWrappers.isEmpty()) {
            handler.accept(ingestDocumentWrappers);
        }

        for (IngestDocument document : ingestDocumentWrappers.stream().map(IngestDocumentWrapper::getIngestDocument).toList()) {
            document.appendFieldValue(field, value, true);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory extends AbstractBatchingSystemProcessor.Factory {
        public static final String PROCESSOR_FACTORY_TYPE = "test_system_processor";
        private final ScriptService scriptService;

        public Factory(ScriptService scriptService) {
            super(PROCESSOR_FACTORY_TYPE);
            this.scriptService = scriptService;
        }

        @Override
        protected AbstractBatchingSystemProcessor newProcessor(String tag, String description, Map<String, Object> config) {
            String processorTag = "TAG";
            String field = "test_field";
            Object value = "valued";
            boolean overrideEnabled = true;
            TemplateScript.Factory compiledTemplate = ConfigurationUtils.compileTemplate(TYPE, processorTag, "field", field, scriptService);
            boolean ignoreEmptyValue = false;
            return new TestSystemProcessor(
                processorTag,
                description,
                compiledTemplate,
                ValueSource.wrap(value, scriptService),
                overrideEnabled,
                ignoreEmptyValue
            );
        }
    }
}
