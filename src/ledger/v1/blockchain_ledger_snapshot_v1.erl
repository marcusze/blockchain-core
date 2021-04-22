-module(blockchain_ledger_snapshot_v1).

-include("blockchain_ledger_snapshot_v1.hrl").
-include("blockchain_vars.hrl").

-export([
         serialize/1,
         deserialize/1,

         is_v6/1,

         snapshot/2,
         snapshot/3,
         import/3,
         load_into_ledger/3,
         load_blocks/3,

         get_blocks/1,
         get_h3dex/1,

         height/1,
         hash/1,

         diff/2
        ]).

-export_type([
    snapshot/0,
    snapshot_v5/0
]).

%% this assumes that everything will have loaded the genesis block
%% already.  I'm not sure that's totally safe in all cases, but it's
%% the right thing for the spots and easy to work around elsewhere.
-define(min_height, 2).

%% this is temporary, something to work with easily while we nail the
%% format and functionality down.  once it's final we can move on to a
%% more permanent and less flexible format, like protobufs, or
%% cauterize.

-type snapshot_v5() ::
    #{
        version => v5,
        current_height => non_neg_integer(),
        transaction_fee =>  non_neg_integer(),
        consensus_members => [libp2p_crypto:pubkey_bin()],

        election_height => non_neg_integer(),
        election_epoch => non_neg_integer(),

        delayed_vars => [term()], % TODO Be more specific
        threshold_txns => [term()], % TODO Be more specific

        master_key => binary(),
        multi_keys => [binary()],
        vars_nonce => pos_integer(),
        vars => [term()], % TODO Be more specific

        gateways => [term()], % TODO Be more specific

        % TODO Key type; Not 100% sure about Val type either...
        pocs => [{Key :: term(), Val :: blockchain_ledger_poc_v1:pocs()}],

        accounts => [term()], % TODO Be more specific
        dc_accounts => [term()], % TODO Be more specific

        security_accounts => [term()], % TODO Be more specific

        htlcs => [term()], % TODO Be more specific

        ouis => [term()], % TODO Be more specific
        subnets => [term()], % TODO Be more specific
        oui_counter => pos_integer(),

        hexes => [term()], % TODO Be more specific
        h3dex => [{integer(), [binary()]}],

        state_channels => [term()], % TODO Be more specific

        blocks => [blockchain_block:block()],

        oracle_price => non_neg_integer(),
        oracle_price_list => [blockchain_ledger_oracle_price_entry:oracle_price_entry()]
    }.

-type snapshot_v6() ::
    {snapshot_v6,
        [
          {version           , v6}
        | {current_height    , non_neg_integer()}
        | {transaction_fee   ,  non_neg_integer()}
        | {consensus_members , [libp2p_crypto:pubkey_bin()]}
        | {election_height   , non_neg_integer()}
        | {election_epoch    , non_neg_integer()}
        | {delayed_vars      , [{term(), term()}]} % TODO Be more specific
        | {threshold_txns    , [term()]} % TODO Be more specific
        | {master_key        , binary()}
        | {multi_keys        , [binary()]}
        | {vars_nonce        , pos_integer()}
        | {vars              , [term()]} % TODO Be more specific
        | {gateways          , binary()}
        | {pocs              , binary()}
        | {accounts          , binary()}
        | {dc_accounts       , binary()}
        | {security_accounts , binary()}
        | {htlcs             , [term()]} % TODO Be more specific
        | {ouis              , [term()]} % TODO Be more specific
        | {subnets           , [term()]} % TODO Be more specific
        | {oui_counter       , pos_integer()}
        | {hexes             , [term()]} % TODO Be more specific
        | {h3dex             , [{integer(), [binary()]}]}
        | {state_channels    , [term()]} % TODO Be more specific
        | {blocks            , [blockchain_block:block()]}
        | {oracle_price      , non_neg_integer()}
        | {oracle_price_list , [blockchain_ledger_oracle_price_entry:oracle_price_entry()]}
        ]}.

-type snapshot() :: snapshot_v6().

-spec snapshot(blockchain_ledger_v1:ledger(), [binary()]) ->
    {ok, snapshot()}
    | {error, term()}.  % TODO More-specific than just term()
snapshot(Ledger0, Blocks) ->
    snapshot(Ledger0, Blocks, delayed).

-spec snapshot(
    blockchain_ledger_v1:ledger(),
    [binary()],
    blockchain_ledger_v1:mode()
) ->
    {ok, snapshot()} | {error, term()}.  % TODO More-specific than just term()
snapshot(Ledger0, Blocks, Mode) ->
    try
        Ledger = blockchain_ledger_v1:mode(Mode, Ledger0),
        {ok, CurrHeight} = blockchain_ledger_v1:current_height(Ledger),
        {ok, ConsensusMembers} = blockchain_ledger_v1:consensus_members(Ledger),
        {ok, ElectionHeight} = blockchain_ledger_v1:election_height(Ledger),
        {ok, ElectionEpoch} = blockchain_ledger_v1:election_epoch(Ledger),
        {ok, MasterKey} = blockchain_ledger_v1:master_key(Ledger),
        MultiKeys = case blockchain_ledger_v1:multi_keys(Ledger) of
                        {ok, Keys} -> Keys;
                        _ -> []
                    end,
        DelayedVars = blockchain_ledger_v1:snapshot_delayed_vars(Ledger),
        ThresholdTxns = blockchain_ledger_v1:snapshot_threshold_txns(Ledger),
        {ok, VarsNonce} = blockchain_ledger_v1:vars_nonce(Ledger),
        Vars = blockchain_ledger_v1:snapshot_vars(Ledger),
        Gateways = blockchain_ledger_v1:snapshot_raw_gateways(Ledger),
        %% need to write these on the ledger side
        PoCs = blockchain_ledger_v1:snapshot_raw_pocs(Ledger),
        Accounts = blockchain_ledger_v1:snapshot_raw_accounts(Ledger),
        DCAccounts = blockchain_ledger_v1:snapshot_raw_dc_accounts(Ledger),
        SecurityAccounts = blockchain_ledger_v1:snapshot_raw_security_accounts(Ledger),

        HTLCs = blockchain_ledger_v1:snapshot_htlcs(Ledger),

        OUIs = blockchain_ledger_v1:snapshot_ouis(Ledger),
        Subnets = blockchain_ledger_v1:snapshot_subnets(Ledger),
        {ok, OUICounter} = blockchain_ledger_v1:get_oui_counter(Ledger),

        Hexes = blockchain_ledger_v1:snapshot_hexes(Ledger),
        H3dex = blockchain_ledger_v1:snapshot_h3dex(Ledger),

        StateChannels = blockchain_ledger_v1:snapshot_state_channels(Ledger),

        {ok, OraclePrice} = blockchain_ledger_v1:current_oracle_price(Ledger),
        {ok, OraclePriceList} = blockchain_ledger_v1:current_oracle_price_list(Ledger),

        Pairs =
            [
                {version          , v6},
                {current_height   , CurrHeight},
                {transaction_fee  ,  0},
                {consensus_members, ConsensusMembers},
                {election_height  , ElectionHeight},
                {election_epoch   , ElectionEpoch},
                {delayed_vars     , DelayedVars},
                {threshold_txns   , ThresholdTxns},
                {master_key       , MasterKey},
                {multi_keys       , MultiKeys},
                {vars_nonce       , VarsNonce},
                {vars             , Vars},
                {gateways         , Gateways},
                {pocs             , PoCs},
                {accounts         , Accounts},
                {dc_accounts      , DCAccounts},
                {security_accounts, SecurityAccounts},
                {htlcs            , HTLCs},
                {ouis             , OUIs},
                {subnets          , Subnets},
                {oui_counter      , OUICounter},
                {hexes            , Hexes},
                {h3dex            , H3dex},
                {state_channels   , StateChannels},
                {blocks           , Blocks},
                {oracle_price     , OraclePrice},
                {oracle_price_list, OraclePriceList}
             ],
        Snapshot = {snapshot_v6, Pairs},
        {ok, Snapshot}
    catch C:E:S ->
            {error, {error_taking_snapshot, C, E, S}}
    end.

-spec frame(pos_integer(), binary()) -> binary().
frame(Vsn, <<Bin/binary>>) ->
    %% simple framing with version, size, & snap
    Siz = byte_size(Bin),
    <<Vsn:8/integer, Siz:32/little-unsigned-integer, Bin/binary>>.

-spec unframe(binary()) -> {pos_integer(), binary()}.
unframe(<<Vsn:8/integer, Siz:32/little-unsigned-integer, Bin:Siz/binary>>) ->
    {Vsn, Bin}.

-spec serialize(snapshot()) ->
    binary().
serialize(Snapshot) ->
    serialize(Snapshot, blocks).

serialize(Snapshot, BlocksP) ->
    serialize_v6(Snapshot, BlocksP).

serialize_v1(Snapshot, noblocks) ->
    %% NOTE: serialize_v1 only gets called with noblocks
    Snapshot1 = Snapshot#blockchain_snapshot_v1{blocks = []},
    frame(1, term_to_binary(Snapshot1, [{compressed, 9}])).

serialize_v2(Snapshot, noblocks) ->
    %% NOTE: serialize_v2 only gets called with noblocks
    Snapshot1 = Snapshot#blockchain_snapshot_v2{blocks = []},
    frame(2, term_to_binary(Snapshot1, [{compressed, 9}])).

serialize_v3(Snapshot, noblocks) ->
    %% NOTE: serialize_v3 only gets called with noblocks
    Snapshot1 = Snapshot#blockchain_snapshot_v3{blocks = []},
    frame(3, term_to_binary(Snapshot1, [{compressed, 9}])).

serialize_v4(Snapshot, noblocks) ->
    %% NOTE: serialize_v4 only gets called with noblocks
    Snapshot1 = Snapshot#blockchain_snapshot_v4{blocks = []},
    frame(4, term_to_binary(Snapshot1, [{compressed, 9}])).

serialize_v5(Snapshot, noblocks) ->
    frame(5, term_to_binary(Snapshot#{blocks => []}, [{compressed, 9}])).

serialize_v6({snapshot_v6, KVL0}, BlocksP) ->
    Key = blocks,
    Blocks =
        case BlocksP of
            blocks ->
                    lists:map(
                        fun (B) when is_tuple(B) ->
                                blockchain_block:serialize(B);
                            (B) -> B
                        end,
                        % TODO Handle version v5: Snapshot#{blocks => []}
                        kvl_get(KVL0, Key, [])
                    );
            noblocks ->
                []
        end,
    frame(6, kvl_to_bin(kvl_set(KVL0, Key, Blocks))).

-spec deserialize(binary()) ->
      {ok, snapshot()}
    | {error, bad_snapshot_binary}.
deserialize(<<Bin/binary>>) ->
    deserialize_(unframe(Bin)).

deserialize_({1, <<BinSnap/binary>>}) ->
    try binary_to_term(BinSnap) of
        OldSnapshot ->
            Snapshot = v4_to_v5(v3_to_v4(v2_to_v3(v1_to_v2(OldSnapshot)))),
            {ok, Snapshot}
    catch _:_ ->
            {error, bad_snapshot_binary}
    end;
deserialize_({2, <<BinSnap/binary>>}) ->
    try binary_to_term(BinSnap) of
        OldSnapshot ->
            Snapshot = v4_to_v5(v3_to_v4(v2_to_v3(OldSnapshot))),
            {ok, Snapshot}
    catch _:_ ->
            {error, bad_snapshot_binary}
    end;
deserialize_({3, <<BinSnap/binary>>}) ->
    try binary_to_term(BinSnap) of
        OldSnapshot ->
            Snapshot = v4_to_v5(v3_to_v4(OldSnapshot)),
            {ok, Snapshot}
    catch _:_ ->
            {error, bad_snapshot_binary}
    end;
deserialize_({4, <<BinSnap/binary>>}) ->
    try binary_to_term(BinSnap) of
        OldSnapshot ->
            Snapshot = v4_to_v5(OldSnapshot),
            {ok, Snapshot}
    catch _:_ ->
            {error, bad_snapshot_binary}
    end;
deserialize_({5, <<BinSnap/binary>>}) ->
    try maps:from_list(binary_to_term(BinSnap)) of
        #{version := v5} = Snapshot ->
            {ok, Snapshot}
    catch _:_ ->
            {error, bad_snapshot_binary}
    end;
deserialize_({6, <<Bin/binary>>}) ->
    {ok, {snapshot_v6, kvl_from_bin(Bin)}}.

%% sha will be stored externally
-spec import(blockchain:blockchain(), binary(), snapshot()) ->
    {ok, blockchain_ledger_v1:ledger()} | {error, bad_snapshot_checksum}.
import(Chain, SHA0, {snapshot_v6, _}=Snapshot) ->
    SHA1 = hash(Snapshot),
    case SHA0 == SHA1 of
        false ->
            {error, bad_snapshot_checksum};
        true ->
            {ok, import_(Chain, SHA1, Snapshot)}
    end;
import(_Chain, SHA, #{version := v5} = Snapshot) ->
    %% TODO deserialize should handle conversion any->latest
    case hash_v5(Snapshot) == SHA orelse
        hash_v4(v5_to_v4(Snapshot)) == SHA orelse
        hash_v3(v4_to_v3(v5_to_v4(Snapshot))) == SHA orelse
        hash_v2(v3_to_v2(v4_to_v3(v5_to_v4(Snapshot)))) == SHA orelse
        hash_v1(v2_to_v1(v3_to_v2(v4_to_v3(v5_to_v4(Snapshot))))) == SHA of
        true ->
            %{ok, import_(Chain, SHA, Snapshot)};
            {error, snapshot_v5_import_not_implemented};
        _ ->
            {error, bad_snapshot_checksum}
    end.

-spec import_(blockchain:blockchain(), binary(), snapshot()) ->
    blockchain_ledger_v1:ledger().
import_(Chain, SHA, {snapshot_v6, KVL}=Snapshot) ->
    Get = fun (K) -> kvl_get_exn(KVL, K) end,
    CLedger = blockchain:ledger(Chain),
    Dir = blockchain:dir(Chain),
    Ledger0 =
        case catch blockchain_ledger_v1:current_height(CLedger) of
            %% nothing in there, proceed
            {ok, 1} ->
                CLedger;
            _ ->
                blockchain_ledger_v1:clean(CLedger),
                blockchain_ledger_v1:new(Dir)
        end,
    Blocks = Get(blocks),
    %% we load up both with the same snapshot here, then sync the next N
    %% blocks and check that we're valid.
    [load_into_ledger(Snapshot, Ledger0, Mode)
     || Mode <- [delayed, active]],
    load_blocks(Ledger0, Chain, Blocks),
    case blockchain_ledger_v1:has_aux(Ledger0) of
        true ->
            load_into_ledger(Snapshot, Ledger0, aux),
            load_blocks(blockchain_ledger_v1:mode(aux, Ledger0), Chain, Blocks);
        false ->
            ok
    end,
    {ok, Curr3} = blockchain_ledger_v1:current_height(Ledger0),
    lager:info("ledger height is ~p after absorbing blocks", [Curr3]),

    %% store the snapshot if we don't have it already
    case blockchain:get_snapshot(SHA, Chain) of
        {ok, _Snap} -> ok;
        {error, not_found} ->
            blockchain:add_snapshot(Snapshot, Chain)
    end,
    Ledger0.

load_into_ledger(#{
         current_height := CurrHeight,
         transaction_fee :=  _TxnFee,
         consensus_members := ConsensusMembers,

         election_height := ElectionHeight,
         election_epoch := ElectionEpoch,

         delayed_vars := DelayedVars,
         threshold_txns := ThresholdTxns,

         master_key := MasterKey,
         multi_keys := MultiKeys,
         vars_nonce := VarsNonce,
         vars := Vars,

         gateways := Gateways,
         pocs := PoCs,

         accounts := Accounts,
         dc_accounts := DCAccounts,

         security_accounts := SecurityAccounts,

         htlcs := HTLCs,

         ouis := OUIs,
         subnets := Subnets,
         oui_counter := OUICounter,

         hexes := Hexes,
         h3dex := H3dex,

         state_channels := StateChannels,

         oracle_price := OraclePrice,
         oracle_price_list := OraclePriceList
         }, Ledger0, Mode) ->
    Ledger1 = blockchain_ledger_v1:mode(Mode, Ledger0),
    Ledger = blockchain_ledger_v1:new_context(Ledger1),
    ok = blockchain_ledger_v1:current_height(CurrHeight, Ledger),
    ok = blockchain_ledger_v1:consensus_members(ConsensusMembers, Ledger),
    ok = blockchain_ledger_v1:election_height(ElectionHeight, Ledger),
    ok = blockchain_ledger_v1:election_epoch(ElectionEpoch, Ledger),
    ok = blockchain_ledger_v1:load_delayed_vars(DelayedVars, Ledger),
    ok = blockchain_ledger_v1:load_threshold_txns(ThresholdTxns, Ledger),
    ok = blockchain_ledger_v1:master_key(MasterKey, Ledger),
    ok = blockchain_ledger_v1:multi_keys(MultiKeys, Ledger),
    ok = blockchain_ledger_v1:vars_nonce(VarsNonce, Ledger),
    ok = blockchain_ledger_v1:load_vars(Vars, Ledger),

    ok = blockchain_ledger_v1:load_raw_gateways(Gateways, Ledger),
    ok = blockchain_ledger_v1:load_raw_pocs(PoCs, Ledger),
    ok = blockchain_ledger_v1:load_raw_accounts(Accounts, Ledger),
    ok = blockchain_ledger_v1:load_raw_dc_accounts(DCAccounts, Ledger),
    ok = blockchain_ledger_v1:load_raw_security_accounts(SecurityAccounts, Ledger),

    ok = blockchain_ledger_v1:load_htlcs(HTLCs, Ledger),

    ok = blockchain_ledger_v1:load_ouis(OUIs, Ledger),
    ok = blockchain_ledger_v1:load_subnets(Subnets, Ledger),
    ok = blockchain_ledger_v1:set_oui_counter(OUICounter, Ledger),

    ok = blockchain_ledger_v1:load_hexes(Hexes, Ledger),
    ok = blockchain_ledger_v1:load_h3dex(H3dex, Ledger),

    ok = blockchain_ledger_v1:load_state_channels(StateChannels, Ledger),

    ok = blockchain_ledger_v1:load_oracle_price(OraclePrice, Ledger),
    ok = blockchain_ledger_v1:load_oracle_price_list(OraclePriceList, Ledger),

    blockchain_ledger_v1:commit_context(Ledger);
load_into_ledger({snapshot_v6, KVL}, L0, Mode) ->
    Get = fun (K) -> kvl_get_exn(KVL, K) end,
    L1 = blockchain_ledger_v1:mode(Mode, L0),
    L = blockchain_ledger_v1:new_context(L1),
    ok = blockchain_ledger_v1:current_height(Get(current_height), L),
    ok = blockchain_ledger_v1:consensus_members(Get(consensus_members), L),
    ok = blockchain_ledger_v1:election_height(Get(election_height), L),
    ok = blockchain_ledger_v1:election_epoch(Get(election_epoch), L),
    ok = blockchain_ledger_v1:load_delayed_vars(Get(delayed_vars), L),
    ok = blockchain_ledger_v1:load_threshold_txns(Get(threshold_txns), L),
    ok = blockchain_ledger_v1:master_key(Get(master_key), L),
    ok = blockchain_ledger_v1:multi_keys(Get(multi_keys), L),
    ok = blockchain_ledger_v1:vars_nonce(Get(vars_nonce), L),
    ok = blockchain_ledger_v1:load_vars(Get(vars), L),

    ok = blockchain_ledger_v1:load_raw_gateways(Get(gateways), L),
    ok = blockchain_ledger_v1:load_raw_pocs(Get(pocs), L),
    ok = blockchain_ledger_v1:load_raw_accounts(Get(accounts), L),
    ok = blockchain_ledger_v1:load_raw_dc_accounts(Get(dc_accounts), L),
    ok = blockchain_ledger_v1:load_raw_security_accounts(Get(security_accounts), L),

    ok = blockchain_ledger_v1:load_htlcs(Get(htlcs), L),

    ok = blockchain_ledger_v1:load_ouis(Get(ouis), L),
    ok = blockchain_ledger_v1:load_subnets(Get(subnets), L),
    ok = blockchain_ledger_v1:set_oui_counter(Get(oui_counter), L),

    ok = blockchain_ledger_v1:load_hexes(Get(hexes), L),
    ok = blockchain_ledger_v1:load_h3dex(Get(h3dex), L),

    ok = blockchain_ledger_v1:load_state_channels(Get(state_channels), L),

    ok = blockchain_ledger_v1:load_oracle_price(Get(oracle_price), L),
    ok = blockchain_ledger_v1:load_oracle_price_list(Get(oracle_price_list), L),
    blockchain_ledger_v1:commit_context(L).

load_blocks(Ledger, Chain, #{blocks:=Blocks}) ->
    load_blocks(Ledger, Chain, Blocks);
load_blocks(Ledger0, Chain, Blocks) ->
    %% TODO: it might make more sense to do this block by block?  it will at least be
    %% cheaper to do it that way.
    Ledger2 = blockchain_ledger_v1:new_context(Ledger0),
    Chain1 = blockchain:ledger(Ledger2, Chain),
    {ok, Curr2} = blockchain_ledger_v1:current_height(Ledger2),
    lager:info("ledger height is ~p before absorbing snapshot", [Curr2]),
    lager:info("snapshot contains ~p blocks", [length(Blocks)]),

    case Blocks of
        [] ->
            %% ignore blocks in testing
            ok;
        [_|_] ->
            %% just store the head, we'll need it sometimes
            lists:foreach(
              fun(Block0) ->
                      Block =
                      case Block0 of
                          B when is_binary(B) ->
                              blockchain_block:deserialize(B);
                          B -> B
                      end,

                      Ht = blockchain_block:height(Block),
                      %% since hash and block are written at the same time, just getting the
                      %% hash from the height is an acceptable presence check, and much cheaper
                      case blockchain:get_block_hash(Ht, Chain) of
                          {ok, _Hash} ->
                              %% already have it, don't need to store it again.
                              ok;
                          _ ->
                              ok = blockchain:save_block(Block, Chain)
                      end,
                      case Ht > Curr2 of
                          %% we need some blocks before for history, only absorb if they're
                          %% not on the ledger already
                          true ->
                              lager:info("loading block ~p", [Ht]),
                              Rescue = blockchain_block:is_rescue_block(Block),
                              {ok, _Chain} = blockchain_txn:absorb_block(Block, Rescue, Chain1),
                              Hash = blockchain_block:hash_block(Block),
                              ok = blockchain_ledger_v1:maybe_gc_pocs(Chain1, Ledger2),

                              ok = blockchain_ledger_v1:maybe_gc_scs(Chain1, Ledger2),

                              ok = blockchain_ledger_v1:refresh_gateway_witnesses(Hash, Ledger2),
                              ok = blockchain_ledger_v1:maybe_recalc_price(Chain1, Ledger2);
                          _ ->
                              ok
                      end
              end,
              Blocks)
    end,
    blockchain_ledger_v1:commit_context(Ledger2).

-spec get_blocks(blockchain:blockchain()) ->
    [binary()].
get_blocks(Chain) ->
    Ledger = blockchain:ledger(Chain),
    {ok, Height} = blockchain_ledger_v1:current_height(Ledger),

    %% this is for rewards calculation when an epoch ends
    %% see https://github.com/helium/blockchain-core/pull/627
    #{ election_height := ElectionHeight } = blockchain_election:election_info(Ledger, Chain),
    {ok, GraceBlocks} = blockchain:config(?sc_grace_blocks, Ledger),

    DLedger = blockchain_ledger_v1:mode(delayed, Ledger),
    {ok, DHeight} = blockchain_ledger_v1:current_height(DLedger),

    %% We need _at least_ the grace blocks before current election
    %% or the delayed ledger height less 181 blocks, whichever is
    %% lower.
    LoadBlockStart = min(DHeight - 181, ElectionHeight - GraceBlocks),

    [begin
         {ok, B} = blockchain:get_raw_block(N, Chain),
         B
     end
     || N <- lists:seq(max(?min_height, LoadBlockStart), Height)].

is_v6({snapshot_v6, _}) -> true;
is_v6(_) -> false.

get_h3dex({snapshot_v6, KVL}) ->
    kvl_get_exn(KVL, h3dex).

height({snapshot_v6, KVL}) ->
    kvl_get_exn(KVL, current_height).

-spec hash(snapshot()) -> binary().
hash({snapshot_v6, _}=Snap) ->
    crypto:hash(sha256, serialize_v6(Snap, noblocks)).

hash_v1(#blockchain_snapshot_v1{} = Snap) ->
    crypto:hash(sha256, serialize_v1(Snap, noblocks)).

hash_v2(#blockchain_snapshot_v2{} = Snap) ->
    crypto:hash(sha256, serialize_v2(Snap, noblocks)).

hash_v3(#blockchain_snapshot_v3{} = Snap) ->
    crypto:hash(sha256, serialize_v3(Snap, noblocks)).

hash_v4(#blockchain_snapshot_v4{} = Snap) ->
    crypto:hash(sha256, serialize_v4(Snap, noblocks)).

hash_v5(#{version := v5} = Snap) ->
    crypto:hash(sha256, serialize_v5(Snap, noblocks)).

v1_to_v2(#blockchain_snapshot_v1{
            previous_snapshot_hash = <<>>,
            leading_hash = <<>>,

            current_height = CurrHeight,
            transaction_fee = _TxnFee,
            consensus_members = ConsensusMembers,

            election_height = ElectionHeight,
            election_epoch = ElectionEpoch,

            delayed_vars = DelayedVars,
            threshold_txns = ThresholdTxns,

            master_key = MasterKey,
            vars_nonce = VarsNonce,
            vars = Vars,

            gateways = Gateways,
            pocs = PoCs,

            accounts = Accounts,
            dc_accounts = DCAccounts,

            %%token_burn_rate = TokenBurnRate,

            security_accounts = SecurityAccounts,

            htlcs = HTLCs,

            ouis = OUIs,
            subnets = Subnets,
            oui_counter = OUICounter,

            hexes = Hexes,

            state_channels = StateChannels,

            blocks = Blocks
           }) ->
    #blockchain_snapshot_v2{
       previous_snapshot_hash = <<>>,
       leading_hash = <<>>,

       current_height = CurrHeight,
       transaction_fee = 0,
       consensus_members = ConsensusMembers,

       election_height = ElectionHeight,
       election_epoch = ElectionEpoch,

       delayed_vars = DelayedVars,
       threshold_txns = ThresholdTxns,

       master_key = MasterKey,
       vars_nonce = VarsNonce,
       vars = Vars,

       gateways = Gateways,
       pocs = PoCs,

       accounts = Accounts,
       dc_accounts = DCAccounts,

       %%token_burn_rate = TokenBurnRate,
       token_burn_rate = 0,

       security_accounts = SecurityAccounts,

       htlcs = HTLCs,

       ouis = OUIs,
       subnets = Subnets,
       oui_counter = OUICounter,

       hexes = Hexes,

       state_channels = StateChannels,

       blocks = Blocks
      }.

v2_to_v1(#blockchain_snapshot_v2{
            previous_snapshot_hash = <<>>,
            leading_hash = <<>>,

            current_height = CurrHeight,
            transaction_fee =  _TxnFee,
            consensus_members = ConsensusMembers,

            election_height = ElectionHeight,
            election_epoch = ElectionEpoch,

            delayed_vars = DelayedVars,
            threshold_txns = ThresholdTxns,

            master_key = MasterKey,
            vars_nonce = VarsNonce,
            vars = Vars,

            gateways = Gateways,
            pocs = PoCs,

            accounts = Accounts,
            dc_accounts = DCAccounts,

            %%token_burn_rate = TokenBurnRate,

            security_accounts = SecurityAccounts,

            htlcs = HTLCs,

            ouis = OUIs,
            subnets = Subnets,
            oui_counter = OUICounter,

            hexes = Hexes,

            state_channels = StateChannels,

            blocks = Blocks
           }) ->
    #blockchain_snapshot_v1{
       previous_snapshot_hash = <<>>,
       leading_hash = <<>>,

       current_height = CurrHeight,
       transaction_fee =  0,
       consensus_members = ConsensusMembers,

       election_height = ElectionHeight,
       election_epoch = ElectionEpoch,

       delayed_vars = DelayedVars,
       threshold_txns = ThresholdTxns,

       master_key = MasterKey,
       vars_nonce = VarsNonce,
       vars = Vars,

       gateways = Gateways,
       pocs = PoCs,

       accounts = Accounts,
       dc_accounts = DCAccounts,

       %%token_burn_rate = TokenBurnRate,
       token_burn_rate = 0,

       security_accounts = SecurityAccounts,

       htlcs = HTLCs,

       ouis = OUIs,
       subnets = Subnets,
       oui_counter = OUICounter,

       hexes = Hexes,

       state_channels = StateChannels,

       blocks = Blocks
      }.


v2_to_v3(#blockchain_snapshot_v2{
            current_height = CurrHeight,
            transaction_fee = _TxnFee,
            consensus_members = ConsensusMembers,

            election_height = ElectionHeight,
            election_epoch = ElectionEpoch,

            delayed_vars = DelayedVars,
            threshold_txns = ThresholdTxns,

            master_key = MasterKey,
            vars_nonce = VarsNonce,
            vars = Vars,

            gateways = Gateways,
            pocs = PoCs,

            accounts = Accounts,
            dc_accounts = DCAccounts,

            security_accounts = SecurityAccounts,

            htlcs = HTLCs,

            ouis = OUIs,
            subnets = Subnets,
            oui_counter = OUICounter,

            hexes = Hexes,

            state_channels = StateChannels,

            blocks = Blocks,

            oracle_price = OraclePrice,
            oracle_price_list = OraclePriceList
           }) ->

    #blockchain_snapshot_v3{
       current_height = CurrHeight,
       transaction_fee = 0,
       consensus_members = ConsensusMembers,

       election_height = ElectionHeight,
       election_epoch = ElectionEpoch,

       delayed_vars = DelayedVars,
       threshold_txns = ThresholdTxns,

       master_key = MasterKey,
       vars_nonce = VarsNonce,
       vars = Vars,

       %% these need to be re-serialized for v3

       gateways = reserialize(fun blockchain_ledger_gateway_v2:serialize/1, Gateways),
       pocs = reserialize_pocs(PoCs),

       accounts = reserialize(fun blockchain_ledger_entry_v1:serialize/1, Accounts),
       dc_accounts = reserialize(fun blockchain_ledger_data_credits_entry_v1:serialize/1, DCAccounts),

       security_accounts = reserialize(fun blockchain_ledger_security_entry_v1:serialize/1, SecurityAccounts),

       %% end re-serialization

       htlcs = HTLCs,

       ouis = OUIs,
       subnets = Subnets,
       oui_counter = OUICounter,

       hexes = Hexes,

       state_channels = StateChannels,

       blocks = Blocks,

       oracle_price = OraclePrice,
       oracle_price_list = OraclePriceList
      }.

v3_to_v4(#blockchain_snapshot_v3{
            current_height = CurrHeight,
            transaction_fee = _TxnFee,
            consensus_members = ConsensusMembers,

            election_height = ElectionHeight,
            election_epoch = ElectionEpoch,

            delayed_vars = DelayedVars,
            threshold_txns = ThresholdTxns,

            master_key = MasterKey,
            vars_nonce = VarsNonce,
            vars = Vars,

            gateways = Gateways,
            pocs = PoCs,

            accounts = Accounts,
            dc_accounts = DCAccounts,

            security_accounts = SecurityAccounts,

            htlcs = HTLCs,

            ouis = OUIs,
            subnets = Subnets,
            oui_counter = OUICounter,

            hexes = Hexes,

            state_channels = StateChannels,

            blocks = Blocks,

            oracle_price = OraclePrice,
            oracle_price_list = OraclePriceList
           }) ->

    #blockchain_snapshot_v4{
       current_height = CurrHeight,
       transaction_fee = 0,
       consensus_members = ConsensusMembers,

       election_height = ElectionHeight,
       election_epoch = ElectionEpoch,

       delayed_vars = DelayedVars,
       threshold_txns = ThresholdTxns,

       master_key = MasterKey,
       multi_keys = [],
       vars_nonce = VarsNonce,
       vars = Vars,

       gateways = Gateways,
       pocs = PoCs,

       accounts = Accounts,
       dc_accounts = DCAccounts,

       security_accounts = SecurityAccounts,

       htlcs = HTLCs,

       ouis = OUIs,
       subnets = Subnets,
       oui_counter = OUICounter,

       hexes = Hexes,

       state_channels = StateChannels,

       blocks = Blocks,

       oracle_price = OraclePrice,
       oracle_price_list = OraclePriceList
      }.

v4_to_v5(#blockchain_snapshot_v4{
            current_height = CurrHeight,
            transaction_fee = _TxnFee,
            consensus_members = ConsensusMembers,

            election_height = ElectionHeight,
            election_epoch = ElectionEpoch,

            delayed_vars = DelayedVars,
            threshold_txns = ThresholdTxns,

            master_key = MasterKey,
            multi_keys = MultiKeys,
            vars_nonce = VarsNonce,
            vars = Vars,

            gateways = Gateways,
            pocs = PoCs,

            accounts = Accounts,
            dc_accounts = DCAccounts,

            security_accounts = SecurityAccounts,

            htlcs = HTLCs,

            ouis = OUIs,
            subnets = Subnets,
            oui_counter = OUICounter,

            hexes = Hexes,

            state_channels = StateChannels,

            blocks = Blocks,

            oracle_price = OraclePrice,
            oracle_price_list = OraclePriceList
           }) ->
    #{
      version => v5,
      current_height => CurrHeight,
      transaction_fee => 0,
      consensus_members => ConsensusMembers,

      election_height => ElectionHeight,
      election_epoch => ElectionEpoch,

      delayed_vars => DelayedVars,
      threshold_txns => ThresholdTxns,

      master_key => MasterKey,
      multi_keys => MultiKeys,
      vars_nonce => VarsNonce,
      vars => Vars,

      gateways => Gateways,
      pocs => PoCs,

      accounts => Accounts,
      dc_accounts => DCAccounts,

      security_accounts => SecurityAccounts,

      htlcs => HTLCs,

      ouis => OUIs,
      subnets => Subnets,
      oui_counter => OUICounter,

      hexes => Hexes,
      h3dex => [],

      state_channels => StateChannels,

      blocks => Blocks,

      oracle_price => OraclePrice,
      oracle_price_list => OraclePriceList
     }.


reserialize(Fun, Values) ->
    lists:map(fun({K, V}) ->
                      {K, Fun(V)}
              end,
              Values).

reserialize_pocs(Values) ->
    lists:map(fun({K, V}) ->
                      List = lists:map(fun blockchain_ledger_poc_v2:serialize/1, V),
                      Value = term_to_binary(List),
                      {K, Value}
              end,
              Values).

v3_to_v2(#blockchain_snapshot_v3{
            current_height = CurrHeight,
            transaction_fee =  _TxnFee,
            consensus_members = ConsensusMembers,

            election_height = ElectionHeight,
            election_epoch = ElectionEpoch,

            delayed_vars = DelayedVars,
            threshold_txns = ThresholdTxns,

            master_key = MasterKey,
            vars_nonce = VarsNonce,
            vars = Vars,

            gateways = Gateways,
            pocs = PoCs,

            accounts = Accounts,
            dc_accounts = DCAccounts,

            security_accounts = SecurityAccounts,

            htlcs = HTLCs,

            ouis = OUIs,
            subnets = Subnets,
            oui_counter = OUICounter,

            hexes = Hexes,

            state_channels = StateChannels,

            blocks = Blocks,

            oracle_price = OraclePrice,
            oracle_price_list = OraclePriceList
           }) ->
    #blockchain_snapshot_v2{
       previous_snapshot_hash = <<>>,
       leading_hash = <<>>,

       current_height = CurrHeight,
       transaction_fee =  0,
       consensus_members = ConsensusMembers,

       election_height = ElectionHeight,
       election_epoch = ElectionEpoch,

       delayed_vars = DelayedVars,
       threshold_txns = ThresholdTxns,

       master_key = MasterKey,
       vars_nonce = VarsNonce,
       vars = Vars,

       %% these need to be deserialized

       gateways = deserialize(fun blockchain_ledger_gateway_v2:deserialize/1, Gateways),
       pocs = deserialize_pocs(PoCs),

       accounts = deserialize(fun blockchain_ledger_entry_v1:deserialize/1, Accounts),
       dc_accounts = deserialize(fun blockchain_ledger_data_credits_entry_v1:deserialize/1, DCAccounts),

       security_accounts = deserialize(fun blockchain_ledger_security_entry_v1:deserialize/1, SecurityAccounts),

       %% end deserialize

       token_burn_rate = 0,

       htlcs = HTLCs,

       ouis = OUIs,
       subnets = Subnets,
       oui_counter = OUICounter,

       hexes = Hexes,

       state_channels = StateChannels,

       blocks = Blocks,

       oracle_price = OraclePrice,
       oracle_price_list = OraclePriceList
      }.

v4_to_v3(#blockchain_snapshot_v4{
            current_height = CurrHeight,
            transaction_fee =  _TxnFee,
            consensus_members = ConsensusMembers,

            election_height = ElectionHeight,
            election_epoch = ElectionEpoch,

            delayed_vars = DelayedVars,
            threshold_txns = ThresholdTxns,

            master_key = MasterKey,
            vars_nonce = VarsNonce,
            vars = Vars,

            gateways = Gateways,
            pocs = PoCs,

            accounts = Accounts,
            dc_accounts = DCAccounts,

            security_accounts = SecurityAccounts,

            htlcs = HTLCs,

            ouis = OUIs,
            subnets = Subnets,
            oui_counter = OUICounter,

            hexes = Hexes,

            state_channels = StateChannels,

            blocks = Blocks,

            oracle_price = OraclePrice,
            oracle_price_list = OraclePriceList
           }) ->
    #blockchain_snapshot_v3{
       current_height = CurrHeight,
       transaction_fee =  _TxnFee,
       consensus_members = ConsensusMembers,

       election_height = ElectionHeight,
       election_epoch = ElectionEpoch,

       delayed_vars = DelayedVars,
       threshold_txns = ThresholdTxns,

       master_key = MasterKey,
       vars_nonce = VarsNonce,
       vars = Vars,

       gateways = Gateways,
       pocs = PoCs,

       accounts = Accounts,
       dc_accounts = DCAccounts,

       security_accounts = SecurityAccounts,

       htlcs = HTLCs,

       ouis = OUIs,
       subnets = Subnets,
       oui_counter = OUICounter,

       hexes = Hexes,

       state_channels = StateChannels,

       blocks = Blocks,

       oracle_price = OraclePrice,
       oracle_price_list = OraclePriceList
      }.

v5_to_v4(#{
           version := v5,
           current_height := CurrHeight,
           consensus_members := ConsensusMembers,

           election_height := ElectionHeight,
           election_epoch := ElectionEpoch,

           delayed_vars := DelayedVars,
           threshold_txns := ThresholdTxns,

           master_key := MasterKey,
           multi_keys := MultiKeys,
           vars_nonce := VarsNonce,
           vars := Vars,

           gateways := Gateways,
           pocs := PoCs,

           accounts := Accounts,
           dc_accounts := DCAccounts,

           security_accounts := SecurityAccounts,

           htlcs := HTLCs,

           ouis := OUIs,
           subnets := Subnets,
           oui_counter := OUICounter,

           hexes := Hexes,
           h3dex := _H3dex,

           state_channels := StateChannels,

           blocks := Blocks,

           oracle_price := OraclePrice,
           oracle_price_list := OraclePriceList}) ->
    #blockchain_snapshot_v4{
       current_height = CurrHeight,
       consensus_members = ConsensusMembers,

       transaction_fee =  0,

       election_height = ElectionHeight,
       election_epoch = ElectionEpoch,

       delayed_vars = DelayedVars,
       threshold_txns = ThresholdTxns,

       master_key = MasterKey,
       multi_keys = MultiKeys,
       vars_nonce = VarsNonce,
       vars = Vars,

       gateways = Gateways,
       pocs = PoCs,

       accounts = Accounts,
       dc_accounts = DCAccounts,

       security_accounts = SecurityAccounts,

       htlcs = HTLCs,

       ouis = OUIs,
       subnets = Subnets,
       oui_counter = OUICounter,

       hexes = Hexes,

       state_channels = StateChannels,

       blocks = Blocks,

       oracle_price = OraclePrice,
       oracle_price_list = OraclePriceList
      }.

deserialize(Fun, Values) ->
    lists:map(fun({K, V}) ->
                      {K, Fun(V)}
              end,
              Values).

deserialize_pocs(Values) ->
    lists:map(fun({K, V}) ->
                      List = binary_to_term(V),
                      Value = lists:map(fun blockchain_ledger_poc_v2:deserialize/1, List),
                      {K, Value}
              end,
              Values).

diff({snapshot_v6, A}, {snapshot_v6, B}) ->
    lists:foldl(
      fun({Field, AI, BI}, Acc) ->
              case AI == BI of
                  true ->
                      Acc;
                  _ ->
                      case Field of
                          F when F == vars; F == security_accounts ->
                              [{Field, AI, BI} | Acc];
                          %% we experience the most drift here, so
                          %% it's worth some effort.
                          gateways ->
                              AUniq = AI -- BI,
                              BUniq = BI -- AI,
                              case diff_gateways(AUniq, BUniq, []) of
                                  [] ->
                                      Acc;
                                  Diff ->
                                      [{gateways, Diff} | Acc]
                              end;
                          blocks ->
                              AHeightAndHash = [ begin
                                                     Block = blockchain_block:deserialize(Block0),
                                                     {blockchain_block:height(Block),
                                                      blockchain_block:hash_block(Block)}
                                                 end
                                                 || Block0 <- AI],
                              BHeightAndHash = [ begin
                                                     Block = blockchain_block:deserialize(Block0),
                                                     {blockchain_block:height(Block),
                                                      blockchain_block:hash_block(Block)}
                                                 end || Block0 <- BI],
                              case {AHeightAndHash -- BHeightAndHash, BHeightAndHash -- AHeightAndHash} of
                                  {[], []} ->
                                      Acc;
                                  {ADiffs, BDiffs} ->
                                      [{Field, [Height || {Height, _Hash} <- ADiffs], [Height || {Height, _Hash} <- BDiffs]} | Acc]
                              end;
                          h3dex ->
                              [{Field, length(AI), length(BI)} | Acc];
                          _ ->
                              [Field | Acc]
                      end
              end
      end,
      [],
      [{K, V, kvl_get_exn(B, K)} || {K, V} <- A]).

diff_gateways([] , [], Acc) ->
    Acc;
diff_gateways(AList , [], Acc) ->
    [lists:map(fun({Addr, _}) -> {Addr, b_missing} end, AList)
     | Acc];
diff_gateways([] , BList, Acc) ->
    [lists:map(fun({Addr, _}) -> {Addr, a_missing} end, BList)
     | Acc];
diff_gateways([{Addr, A} | T] , BList, Acc) ->
    case gwget(Addr, BList) of
        missing ->
            diff_gateways(T, lists:keydelete(Addr, 1, BList),
                          [{Addr, b_missing} | Acc]);
        B ->
            %% sometimes map encoding lies to us
            case minimize_gw(A, B) of
                [] ->
                    diff_gateways(T, lists:keydelete(Addr, 1, BList),
                                  Acc);
                MiniGw ->
                    diff_gateways(T, lists:keydelete(Addr, 1, BList),
                                  [{Addr, MiniGw} | Acc])
            end
    end.

gwget(Addr, L) ->
    case lists:keyfind(Addr, 1, L) of
        {_, GW} ->
            GW;
        false ->
            missing
    end.

minimize_gw(A0, B0) ->
    A = blockchain_ledger_gateway_v2:deserialize(A0),
    B = blockchain_ledger_gateway_v2:deserialize(B0),
    %% We can directly compare some fields
    Compare =
        lists:flatmap(
          fun(Fn) ->
                  AVal = blockchain_ledger_gateway_v2:Fn(A),
                  BVal = blockchain_ledger_gateway_v2:Fn(B),
                  case AVal == BVal of
                      true ->
                          [];
                      false ->
                          [{Fn, AVal, BVal}]
                  end
          end,
          [location, version, last_poc_challenge, last_poc_onion_key_hash,
           nonce, alpha, beta, delta, oui]),
    %% but for witnesses, we want to do additional minimization
    AWits = blockchain_ledger_gateway_v2:witnesses(A),
    BWits = blockchain_ledger_gateway_v2:witnesses(B),
    %% we do a more detailed comparison here, which can sometimes
    %% reveal encoding differences :/
    case minimize_witnesses(AWits, BWits) of
        [] -> Compare;
        MiniWit ->
            [{witnesses, MiniWit} | Compare]
    end.

minimize_witnesses(A, B) ->
    Compare =
        maps:fold(
          fun(Addr, AWit, Acc) ->
                  case maps:get(Addr, B, missing) of
                      missing ->
                          [{Addr, b_missing} | Acc];
                      BWit ->
                          case BWit == AWit of
                              true ->
                                  Acc;
                              false ->
                                  %% we could probably do more here,
                                  %% narrowing down to counts/histograms/whatever
                                  [{Addr, AWit, BWit} | Acc]
                          end
                  end
          end,
          [], A),
    AKeys = maps:keys(A),
    B1 = maps:without(AKeys, B),
    case maps:size(B1) of
        0 ->
            Compare;
        _ ->
            AMissing =
                maps:fold(fun(K, _V, Acc) ->
                                  [{K, a_missing} | Acc]
                          end, [], B1),
            [AMissing | Compare]
    end.

-spec kvl_get_exn([{K, V}], K) -> V.
kvl_get_exn(KVL, K) ->
    {value, {K, V}} = lists:keysearch(K, 1, KVL),
    V.

-spec kvl_get([{K, V}], K, V) -> V.
kvl_get(KVL, K, Default) ->
    case lists:keysearch(K, 1, KVL) of
        {value, {K, V}} -> V;
        false -> Default
    end.

-spec kvl_set([{K, V}], K, V) -> [{K, V}].
kvl_set(KVL, K, V) ->
    lists:keyreplace(K, 1, KVL, {K, V}).

-spec kvl_from_bin(binary()) ->
    [{term(), term()}].
kvl_from_bin(Bin) ->
    kvl_from_bin(Bin, []).

kvl_from_bin(<<>>, KVL) ->
    lists:reverse(KVL);
kvl_from_bin(<<Siz:32/integer-unsigned-little, Bin:Siz/binary, Rest/binary>>, KVL) ->
    {_, _}=KV = binary_to_term(Bin),
    kvl_from_bin(Rest, [KV | KVL]).

kvl_to_bin(KVL) ->
    iolist_to_binary(lists:map(fun kv_to_bin/1, KVL)).

-spec kv_to_bin({_, _}) -> <<_:32,_:_*8>>.
kv_to_bin({_, _}=KV) ->
    Bin = term_to_binary(KV),
    Siz = byte_size(Bin),
    <<Siz:32/integer-unsigned-little, Bin/binary>>.
