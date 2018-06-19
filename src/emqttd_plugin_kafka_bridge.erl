%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. (http://emqtt.io)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqttd_plugin_kafka_bridge).

-include_lib("../../emqttd/include/emqttd.hrl").

-export([load/1, unload/0]).

%% Hooks functions

-export([on_client_connected/3, on_client_disconnected/3]).

-export([on_client_subscribe/4, on_client_unsubscribe/4]).

-export([on_message_publish/2, on_message_delivered/4, on_message_acked/4]).

%% Called when the plugin application start
load(Env) ->
    ekaf_init([Env]),
    emqttd:hook('client.connected', fun ?MODULE:on_client_connected/3, [Env]),
    emqttd:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]),
    emqttd:hook('client.subscribe', fun ?MODULE:on_client_subscribe/4, [Env]),
    emqttd:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4, [Env]),
    emqttd:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]),
    emqttd:hook('message.delivered', fun ?MODULE:on_message_delivered/4, [Env]),
    emqttd:hook('message.acked', fun ?MODULE:on_message_acked/4, [Env]).

on_client_connected(ConnAck, Client = #mqtt_client{client_id = ClientId}, _Env) ->
    io:format("client ~s connected, connack: ~w~n", [ClientId, ConnAck]),
    {ok, Client}.

on_client_disconnected(Reason, _Client = #mqtt_client{client_id = ClientId}, _Env) ->
    io:format("client ~s disconnected, reason: ~w~n", [ClientId, Reason]),
    ok.

on_client_subscribe(ClientId,Username ,TopicTable, _Env) ->
    io:format("client ~s will subscribe ~p~n", [ClientId, TopicTable]),

    case TopicTable of
        [_|_] ->
            %% If TopicTable list is not empty
            Key = proplists:get_keys(TopicTable),
            %% build json to send using ClientId
            Json = mochijson2:encode([
                {type, <<"subscribed">>},
                {client_id, ClientId},
                {topic, lists:last(Key)},
                {cluster_node, node()}
            ]),
            ekaf:produce_sync(<<"broker_message">>, list_to_binary(Json));
        _ ->
            %% If TopicTable is empty
            io:format("empty topic ~n")
    end,

    {ok, TopicTable}.

on_client_unsubscribe(ClientId,Username, Topics, _Env) ->
    io:format("client ~s unsubscribe ~p~n", [ClientId, Topics]),
    {ok, Topics}.

%% transform message and return
on_message_publish(Message = #mqtt_message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message, _Env) ->
    io:format("publish ~s~n", [emqttd_message:format(Message)]),

%    From = Message#mqtt_message.from,
%    Sender =  Message#mqtt_message.sender,
    Topic = Message#mqtt_message.topic,
    Payload = Message#mqtt_message.payload,
    QoS = Message#mqtt_message.qos,
    Timestamp = Message#mqtt_message.timestamp,

    Json = mochijson2:encode([
        {type, <<"published">>},
%        {client_id, From},
        {topic, Topic},
% 如果是二进制 {payload, binary_to_list(Payload)},
        {payload, Payload},
        {qos, QoS},
        {cluster_node, node()}
%        ,{ts, emqttd_time:now_to_secs(Timestamp)}
    ]),

%%    ekaf:produce_sync(<<"broker_message">>, list_to_binary(Json)),
    ekaf:produce_sync(<<"broker_message">>, Json),

    {ok, Message}.

on_message_delivered(ClientId,Username, Message, _Env) ->
    io:format("delivered to client ~s: ~s~n", [ClientId, emqttd_message:format(Message)]),

%    From = Message#mqtt_message.from,
%    Sender =  Message#mqtt_message.sender,
%%    Topic = Message#mqtt_message.topic,
%%    Payload = Message#mqtt_message.payload,
%%    QoS = Message#mqtt_message.qos,
%%    Timestamp = Message#mqtt_message.timestamp,

%%    Json = mochijson2:encode([
%%        {type, <<"delivered">>},
%%        {client_id, ClientId},
%%%        {from, From},
%%        {topic, Topic},
%%% 如果是二进制 {payload, binary_to_list(Payload)},
%%        {payload, Payload},
%%        {qos, QoS},
%%        {cluster_node, node()}
%%%        ,{ts, emqttd_time:now_to_secs(Timestamp)}
%%    ]),

%%    ekaf:produce_sync(<<"broker_message">>, list_to_binary(Json)),

    {ok, Message}.

on_message_acked(ClientId,Username, Message, _Env) ->
    io:format("client ~s acked: ~s~n", [ClientId, emqttd_message:format(Message)]),

%    From = Message#mqtt_message.from,
%    Sender =  Message#mqtt_message.sender,
%%    Topic = Message#mqtt_message.topic,
%%    Payload = Message#mqtt_message.payload,
%%    QoS = Message#mqtt_message.qos,
%%    Timestamp = Message#mqtt_message.timestamp,

%%    Json = mochijson2:encode([
%%        {type, <<"acked">>},
%%        {client_id, ClientId},
%%%        {from, From},
%%        {topic, Topic},
%%% 如果是二进制 {payload, binary_to_list(Payload)},
%%        {payload, Payload},
%%        {qos, QoS},
%%        {cluster_node, node()}
%%%        ,{ts, emqttd_time:now_to_secs(Timestamp)}
%%    ]),

%%    ekaf:produce_sync(<<"broker_message">>, list_to_binary(Json)),

    {ok, Message}.

%% Called when the plugin application stop
unload() ->
    emqttd:unhook('client.connected', fun ?MODULE:on_client_connected/3),
    emqttd:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3),
    emqttd:unhook('client.subscribe', fun ?MODULE:on_client_subscribe/3),
    emqttd:unhook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/3),
    emqttd:unhook('message.publish', fun ?MODULE:on_message_publish/2),
    emqttd:unhook('message.delivered', fun ?MODULE:on_message_delivered/3),
    emqttd:unhook('message.acked', fun ?MODULE:on_message_acked/3).

%% ==================== ekaf_init STA.===============================%%
ekaf_init(_Env) ->
    %% Get parameters
    {ok, Kafka} = application:get_env(emqttd_plugin_kafka_bridge, kafka),
    BootstrapBroker = proplists:get_value(bootstrap_broker, Kafka),
    PartitionStrategy= proplists:get_value(partition_strategy, Kafka),
    %% Set partition strategy, like application:set_env(ekaf, ekaf_partition_strategy, strict_round_robin),
    application:set_env(ekaf, ekaf_partition_strategy, PartitionStrategy),
    %% Set broker url and port, like application:set_env(ekaf, ekaf_bootstrap_broker, {"127.0.0.1", 9092}),
    application:set_env(ekaf, ekaf_bootstrap_broker, BootstrapBroker),
    %% Set topic
    application:set_env(ekaf, ekaf_bootstrap_topics, <<"broker_message">>),

    {ok, _} = application:ensure_all_started(kafkamocker),
    {ok, _} = application:ensure_all_started(gproc),
    {ok, _} = application:ensure_all_started(ranch),
    {ok, _} = application:ensure_all_started(ekaf),

    io:format("Init ekaf with ~p~n", [BootstrapBroker]).
%% ==================== ekaf_init END.===============================%%