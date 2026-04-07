package com.clickhouse.example.arrow_server;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;

public class ServerDataProducer implements FlightProducer {
    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {

    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {

    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        return null;
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {

    }

    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {

    }
}
