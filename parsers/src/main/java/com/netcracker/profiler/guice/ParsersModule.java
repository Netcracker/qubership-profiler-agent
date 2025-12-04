package com.netcracker.profiler.guice;

import com.netcracker.profiler.fetch.FetchCallTree;
import com.netcracker.profiler.fetch.FetchCallTreeFactory;
import com.netcracker.profiler.io.*;
import com.netcracker.profiler.io.call.*;
import com.netcracker.profiler.io.searchconditions.BaseSearchConditions;
import com.netcracker.profiler.io.searchconditions.BaseSearchConditionsFactory;
import com.netcracker.profiler.io.xlsx.AggregateCallsToXLSXListener;
import com.netcracker.profiler.io.xlsx.AggregateCallsToXLSXListenerFactory;
import com.netcracker.profiler.io.xlsx.CallsToXLSXListener;
import com.netcracker.profiler.io.xlsx.CallsToXLSXListenerFactory;
import com.netcracker.profiler.sax.builders.MultiRangeSuspendLogBuilder;
import com.netcracker.profiler.sax.builders.MultiRangeSuspendLogBuilderFactory;
import com.netcracker.profiler.sax.builders.SuspendLogBuilder;
import com.netcracker.profiler.sax.builders.SuspendLogBuilderFactory;
import com.netcracker.profiler.sax.readers.*;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.MapBinder;

import java.io.File;

/**
 * Guice module for parsers package.
 * Configures dependency injection for profiler parsers and related components.
 */
public class ParsersModule extends AbstractModule {
    private final File serverDumpLocation;

    public ParsersModule(File serverDumpLocation) {
        this.serverDumpLocation = serverDumpLocation;
    }

    @Provides
    @Singleton
    @DumpRootLocation
    File provideDumpRootLocation() {
        return serverDumpLocation;
    }

    @Provides
    @Singleton
    @DumpServerLocation
    File provideDumpServerLocation() {
        return serverDumpLocation.getParentFile();
    }

    @Override
    protected void configure() {
        // Singleton components (that don't depend on factories)
        // Scope is defined by @Singleton annotation on the classes themselves
        bind(CallReaderFactory.class).to(CallReaderFactoryFile.class);
        bind(IDumpExporter.class).to(DumpExporterFile.class);
        bind(IActivePODReporter.class).to(EmptyActivePODReporter.class);

        // Assisted injection for prototype beans with constructor parameters

        install(new FactoryModuleBuilder()
                .implement(ParamReader.class, ParamReaderFile.getBestImplementation())
                .build(ParamReaderFileFactory.class));

        install(new FactoryModuleBuilder()
                .implement(FetchCallTree.class, FetchCallTree.class)
                .build(FetchCallTreeFactory.class));

        install(new FactoryModuleBuilder()
                .implement(CallReaderFile.class, CallReaderFile.class)
                .build(CallReaderFileFactory.class));

        install(new FactoryModuleBuilder()
                .implement(CallToJS.class, CallToJS.class)
                .build(CallToJSFactory.class));

        install(new FactoryModuleBuilder()
                .implement(SuspendLogReader.class, SuspendLogReader.class)
                .build(SuspendLogReaderFactory.class));

        install(new FactoryModuleBuilder()
                .implement(SuspendLogBuilder.class, SuspendLogBuilder.class)
                .build(SuspendLogBuilderFactory.class));

        install(new FactoryModuleBuilder()
                .implement(MultiRangeSuspendLogBuilder.class, MultiRangeSuspendLogBuilder.class)
                .build(MultiRangeSuspendLogBuilderFactory.class));

        install(new FactoryModuleBuilder()
                .implement(BaseSearchConditions.class, BaseSearchConditions.class)
                .build(BaseSearchConditionsFactory.class));

        install(new FactoryModuleBuilder()
                .implement(AggregateCallsToXLSXListener.class, AggregateCallsToXLSXListener.class)
                .build(AggregateCallsToXLSXListenerFactory.class));

        install(new FactoryModuleBuilder()
                .implement(CallsToXLSXListener.class, CallsToXLSXListener.class)
                .build(CallsToXLSXListenerFactory.class));

        install(new FactoryModuleBuilder()
                .implement(ProfilerTraceReaderMR.class, ProfilerTraceReaderMR.class)
                .build(ProfilerTraceReaderMRFactory.class));

        install(new FactoryModuleBuilder()
                .implement(ProfilerTraceReader.class, ProfilerTraceReaderFile.class)
                .build(ProfilerTraceReaderFactory.class));

        install(new FactoryModuleBuilder()
                .implement(InFlightCallsFromTraceScanner.class, InFlightCallsFromTraceScanner.class)
                .build(InFlightCallsFromTraceScannerFactory.class));

        // Map of file format versions to CallDataReader implementations
        MapBinder<Integer, CallDataReader> callDataReaderBinder =
                MapBinder.newMapBinder(binder(), Integer.class, CallDataReader.class);
        callDataReaderBinder.addBinding(0).to(CallDataReader_00.class);
        callDataReaderBinder.addBinding(1).to(CallDataReader_01.class);
        callDataReaderBinder.addBinding(2).to(CallDataReader_02.class);
        callDataReaderBinder.addBinding(3).to(CallDataReader_03.class);
        callDataReaderBinder.addBinding(4).to(CallDataReader_04.class);

        // Bind CallDataReaderFactory
        bind(CallDataReaderFactory.class);
    }
}
