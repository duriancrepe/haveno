/*
 * This file is part of Haveno.
 *
 * Haveno is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at
 * your option) any later version.
 *
 * Haveno is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Haveno. If not, see <http://www.gnu.org/licenses/>.
 */

package bisq.core.trade;

import bisq.core.api.CoreNotificationService;
import bisq.core.btc.model.XmrAddressEntry;
import bisq.core.btc.wallet.XmrWalletService;
import bisq.core.locale.Res;
import bisq.core.offer.Offer;
import bisq.core.offer.OfferBookService;
import bisq.core.offer.OfferDirection;
import bisq.core.offer.OfferUtil;
import bisq.core.offer.OpenOffer;
import bisq.core.offer.OpenOfferManager;
import bisq.core.offer.SignedOffer;
import bisq.core.offer.availability.OfferAvailabilityModel;
import bisq.core.provider.fee.FeeService;
import bisq.core.provider.price.PriceFeedService;
import bisq.core.support.dispute.arbitration.arbitrator.Arbitrator;
import bisq.core.support.dispute.arbitration.arbitrator.ArbitratorManager;
import bisq.core.support.dispute.mediation.mediator.MediatorManager;
import bisq.core.trade.Trade.Phase;
import bisq.core.trade.failed.FailedTradesManager;
import bisq.core.trade.handlers.TradeResultHandler;
import bisq.core.trade.messages.DepositRequest;
import bisq.core.trade.messages.DepositResponse;
import bisq.core.trade.messages.InitMultisigRequest;
import bisq.core.trade.messages.InitTradeRequest;
import bisq.core.trade.messages.PaymentAccountKeyRequest;
import bisq.core.trade.messages.SignContractRequest;
import bisq.core.trade.messages.SignContractResponse;
import bisq.core.trade.messages.UpdateMultisigRequest;
import bisq.core.trade.protocol.ArbitratorProtocol;
import bisq.core.trade.protocol.MakerProtocol;
import bisq.core.trade.protocol.ProcessModel;
import bisq.core.trade.protocol.ProcessModelServiceProvider;
import bisq.core.trade.protocol.TakerProtocol;
import bisq.core.trade.protocol.TradeProtocol;
import bisq.core.trade.protocol.TradeProtocolFactory;
import bisq.core.trade.protocol.TraderProtocol;
import bisq.core.trade.statistics.ReferralIdService;
import bisq.core.trade.statistics.TradeStatisticsManager;
import bisq.core.user.User;
import bisq.core.util.Validator;
import bisq.core.util.coin.CoinUtil;
import bisq.network.p2p.BootstrapListener;
import bisq.network.p2p.DecryptedDirectMessageListener;
import bisq.network.p2p.DecryptedMessageWithPubKey;
import bisq.network.p2p.NodeAddress;
import bisq.network.p2p.P2PService;
import bisq.network.p2p.network.TorNetworkNode;
import com.google.common.collect.ImmutableList;
import bisq.common.ClockWatcher;
import bisq.common.config.Config;
import bisq.common.crypto.KeyRing;
import bisq.common.handlers.ErrorMessageHandler;
import bisq.common.handlers.FaultHandler;
import bisq.common.handlers.ResultHandler;
import bisq.common.persistence.PersistenceManager;
import bisq.common.proto.network.NetworkEnvelope;
import bisq.common.proto.persistable.PersistedDataHost;

import org.bitcoinj.core.Coin;

import javax.inject.Inject;
import javax.inject.Named;

import javafx.beans.property.BooleanProperty;
import javafx.beans.property.LongProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleLongProperty;

import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;

import org.bouncycastle.crypto.params.KeyParameter;
import org.fxmisc.easybind.EasyBind;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import monero.daemon.model.MoneroTx;
import monero.wallet.model.MoneroOutputQuery;


public class TradeManager implements PersistedDataHost, DecryptedDirectMessageListener {
    private static final Logger log = LoggerFactory.getLogger(TradeManager.class);

    private final User user;
    @Getter
    private final KeyRing keyRing;
    private final XmrWalletService xmrWalletService;
    private final CoreNotificationService notificationService;
    private final OfferBookService offerBookService;
    private final OpenOfferManager openOfferManager;
    private final ClosedTradableManager closedTradableManager;
    private final FailedTradesManager failedTradesManager;
    private final P2PService p2PService;
    private final PriceFeedService priceFeedService;
    private final TradeStatisticsManager tradeStatisticsManager;
    private final OfferUtil offerUtil;
    private final TradeUtil tradeUtil;
    @Getter
    private final ArbitratorManager arbitratorManager;
    private final MediatorManager mediatorManager;
    private final ProcessModelServiceProvider processModelServiceProvider;
    private final ClockWatcher clockWatcher;

    private final Map<String, TradeProtocol> tradeProtocolByTradeId = new HashMap<>();
    private final PersistenceManager<TradableList<Trade>> persistenceManager;
    private final TradableList<Trade> tradableList = new TradableList<>();
    @Getter
    private final BooleanProperty persistedTradesInitialized = new SimpleBooleanProperty();
    @Setter
    @Nullable
    private ErrorMessageHandler takeOfferRequestErrorMessageHandler;
    @Getter
    private final LongProperty numPendingTrades = new SimpleLongProperty();
    private final ReferralIdService referralIdService;


    ///////////////////////////////////////////////////////////////////////////////////////////
    // Constructor
    ///////////////////////////////////////////////////////////////////////////////////////////

    @Inject
    public TradeManager(User user,
                        KeyRing keyRing,
                        XmrWalletService xmrWalletService,
                        CoreNotificationService notificationService,
                        OfferBookService offerBookService,
                        OpenOfferManager openOfferManager,
                        ClosedTradableManager closedTradableManager,
                        FailedTradesManager failedTradesManager,
                        P2PService p2PService,
                        PriceFeedService priceFeedService,
                        TradeStatisticsManager tradeStatisticsManager,
                        OfferUtil offerUtil,
                        TradeUtil tradeUtil,
                        ArbitratorManager arbitratorManager,
                        MediatorManager mediatorManager,
                        ProcessModelServiceProvider processModelServiceProvider,
                        ClockWatcher clockWatcher,
                        PersistenceManager<TradableList<Trade>> persistenceManager,
                        ReferralIdService referralIdService) {
        this.user = user;
        this.keyRing = keyRing;
        this.xmrWalletService = xmrWalletService;
        this.notificationService = notificationService;
        this.offerBookService = offerBookService;
        this.openOfferManager = openOfferManager;
        this.closedTradableManager = closedTradableManager;
        this.failedTradesManager = failedTradesManager;
        this.p2PService = p2PService;
        this.priceFeedService = priceFeedService;
        this.tradeStatisticsManager = tradeStatisticsManager;
        this.offerUtil = offerUtil;
        this.tradeUtil = tradeUtil;
        this.arbitratorManager = arbitratorManager;
        this.mediatorManager = mediatorManager;
        this.processModelServiceProvider = processModelServiceProvider;
        this.clockWatcher = clockWatcher;
        this.referralIdService = referralIdService;
        this.persistenceManager = persistenceManager;

        this.persistenceManager.initialize(tradableList, "PendingTrades", PersistenceManager.Source.PRIVATE);

        p2PService.addDecryptedDirectMessageListener(this);

        failedTradesManager.setUnFailTradeCallback(this::unFailTrade);
    }


    ///////////////////////////////////////////////////////////////////////////////////////////
    // PersistedDataHost
    ///////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public void readPersisted(Runnable completeHandler) {
        persistenceManager.readPersisted(persisted -> {
                    tradableList.setAll(persisted.getList());
                    tradableList.stream()
                            .filter(trade -> trade.getOffer() != null)
                            .forEach(trade -> trade.getOffer().setPriceFeedService(priceFeedService));
                    completeHandler.run();
                },
                completeHandler);
    }


    ///////////////////////////////////////////////////////////////////////////////////////////
    // DecryptedDirectMessageListener
    ///////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public void onDirectMessage(DecryptedMessageWithPubKey message, NodeAddress peer) {
        NetworkEnvelope networkEnvelope = message.getNetworkEnvelope();
        if (networkEnvelope instanceof InitTradeRequest) {
            handleInitTradeRequest((InitTradeRequest) networkEnvelope, peer);
        } else if (networkEnvelope instanceof InitMultisigRequest) {
            handleInitMultisigRequest((InitMultisigRequest) networkEnvelope, peer);
        } else if (networkEnvelope instanceof SignContractRequest) {
            handleSignContractRequest((SignContractRequest) networkEnvelope, peer);
        } else if (networkEnvelope instanceof SignContractResponse) {
            handleSignContractResponse((SignContractResponse) networkEnvelope, peer);
        } else if (networkEnvelope instanceof DepositRequest) {
            handleDepositRequest((DepositRequest) networkEnvelope, peer);
        } else if (networkEnvelope instanceof DepositResponse) {
            handleDepositResponse((DepositResponse) networkEnvelope, peer);
        } else if (networkEnvelope instanceof PaymentAccountKeyRequest) {
            handlePaymentAccountKeyRequest((PaymentAccountKeyRequest) networkEnvelope, peer);
        } else if (networkEnvelope instanceof UpdateMultisigRequest) {
            handleUpdateMultisigRequest((UpdateMultisigRequest) networkEnvelope, peer);
        }
    }


    ///////////////////////////////////////////////////////////////////////////////////////////
    // Lifecycle
    ///////////////////////////////////////////////////////////////////////////////////////////

    public void onAllServicesInitialized() {
        if (p2PService.isBootstrapped()) {
            initPersistedTrades();
        } else {
            p2PService.addP2PServiceListener(new BootstrapListener() {
                @Override
                public void onUpdatedDataReceived() {
                    initPersistedTrades();
                }
            });
        }

        getObservableList().addListener((ListChangeListener<Trade>) change -> onTradesChanged());
        onTradesChanged();

        xmrWalletService.setTradeManager(this);
        xmrWalletService.getAddressEntriesForAvailableBalanceStream()
                .filter(addressEntry -> addressEntry.getOfferId() != null)
                .forEach(addressEntry -> {
                    log.warn("Swapping pending {} entries at startup. offerId={}", addressEntry.getContext(), addressEntry.getOfferId());
                    xmrWalletService.swapTradeEntryToAvailableEntry(addressEntry.getOfferId(), XmrAddressEntry.Context.OFFER_FUNDING);
                });

        // thaw unreserved outputs
        thawUnreservedOutputs();
    }

    private void thawUnreservedOutputs() {
        if (xmrWalletService.getWallet() == null) return;

        // collect reserved outputs
        Set<String> reservedKeyImages = new HashSet<String>();
        for (Trade trade : getObservableList()) {
            if (trade.getSelf().getReserveTxKeyImages() == null) continue;
            reservedKeyImages.addAll(trade.getSelf().getReserveTxKeyImages());
        }
        for (OpenOffer openOffer : openOfferManager.getObservableList()) {
            reservedKeyImages.addAll(openOffer.getOffer().getOfferPayload().getReserveTxKeyImages());
        }

        // thaw unreserved outputs
        Set<String> frozenKeyImages = xmrWalletService.getWallet().getOutputs(new MoneroOutputQuery()
                .setIsFrozen(true)
                .setIsSpent(false))
                .stream().map(output -> output.getKeyImage().getHex())
                .collect(Collectors.toSet());
        frozenKeyImages.removeAll(reservedKeyImages);
        for (String unreservedFrozenKeyImage : frozenKeyImages) {
            log.info("Thawing output which is not reserved for offer or trade: " + unreservedFrozenKeyImage);
            xmrWalletService.getWallet().thawOutput(unreservedFrozenKeyImage);
        }
    }

    public TradeProtocol getTradeProtocol(Trade trade) {
        String uid = trade.getUid();
        if (tradeProtocolByTradeId.containsKey(uid)) {
            return tradeProtocolByTradeId.get(uid);
        } else {
            TradeProtocol tradeProtocol = TradeProtocolFactory.getNewTradeProtocol(trade);
            TradeProtocol prev = tradeProtocolByTradeId.put(uid, tradeProtocol);
            if (prev != null) {
                log.error("We had already an entry with uid {}", trade.getUid());
            }

            return tradeProtocol;
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////
    // Init pending trade
    ///////////////////////////////////////////////////////////////////////////////////////////

    private void initPersistedTrades() {

        // open trades in parallel since each may open a multisig wallet
        List<Trade> trades = tradableList.getList();
        if (!trades.isEmpty()) {
            ExecutorService pool = Executors.newFixedThreadPool(Math.min(10, trades.size()));
            for (Trade trade : trades) {
                pool.submit(new Runnable() {
                    @Override
                    public void run() {
                        initPersistedTrade(trade);
                    }
                });
            }
            pool.shutdown();
            try {
                if (!pool.awaitTermination(60000, TimeUnit.SECONDS)) pool.shutdownNow();
            } catch (InterruptedException e) {
                pool.shutdownNow();
                throw new RuntimeException(e);
            }
        }

        persistedTradesInitialized.set(true);

        // We do not include failed trades as they should not be counted anyway in the trade statistics
        Set<Trade> allTrades = new HashSet<>(closedTradableManager.getClosedTrades());
        allTrades.addAll(tradableList.getList());
        String referralId = referralIdService.getOptionalReferralId().orElse(null);
        boolean isTorNetworkNode = p2PService.getNetworkNode() instanceof TorNetworkNode;
        tradeStatisticsManager.maybeRepublishTradeStatistics(allTrades, referralId, isTorNetworkNode);
    }

    private void initPersistedTrade(Trade trade) {
        initTradeAndProtocol(trade, getTradeProtocol(trade));
        requestPersistence();
    }

    private void initTradeAndProtocol(Trade trade, TradeProtocol tradeProtocol) {
        tradeProtocol.initialize(processModelServiceProvider, this, trade.getOffer());
        trade.initialize(processModelServiceProvider);
        requestPersistence(); // TODO requesting persistence twice with initPersistedTrade()
    }

    public void requestPersistence() {
        persistenceManager.requestPersistence();
    }

    private void handleInitTradeRequest(InitTradeRequest request, NodeAddress sender) {
      log.info("Received InitTradeRequest from {} with tradeId {} and uid {}", sender, request.getTradeId(), request.getUid());

      try {
          Validator.nonEmptyStringOf(request.getTradeId());
      } catch (Throwable t) {
          log.warn("Invalid InitTradeRequest message " + request.toString());
          return;
      }

      // handle request as arbitrator
      boolean isArbitrator = request.getArbitratorNodeAddress().equals(p2PService.getNetworkNode().getNodeAddress());
      if (isArbitrator) {

          // verify this node is registered arbitrator
          Arbitrator thisArbitrator = user.getRegisteredArbitrator();
          NodeAddress thisAddress = p2PService.getNetworkNode().getNodeAddress();
          if (thisArbitrator == null || !thisArbitrator.getNodeAddress().equals(thisAddress)) {
              log.warn("Ignoring InitTradeRequest from {} with tradeId {} because we are not an arbitrator", sender, request.getTradeId());
              return;
          }

          // get offer associated with trade
          Offer offer = null;
          for (Offer anOffer : offerBookService.getOffers()) {
              if (anOffer.getId().equals(request.getTradeId())) {
                  offer = anOffer;
              }
          }
          if (offer == null) {
              log.warn("Ignoring InitTradeRequest from {} with tradeId {} because no offer is on the books", sender, request.getTradeId());
              return;
          }

          // verify arbitrator is payload signer unless they are offline
          // TODO (woodser): handle if payload signer differs from current arbitrator (verify signer is offline)

          // verify maker is offer owner
          // TODO (woodser): maker address might change if they disconnect and reconnect, should allow maker address to differ if pubKeyRing is same ?
          if (!offer.getOwnerNodeAddress().equals(request.getMakerNodeAddress())) {
              log.warn("Ignoring InitTradeRequest from {} with tradeId {} because maker is not offer owner", sender, request.getTradeId());
              return;
          }

          Trade trade;
          Optional<Trade> tradeOptional = getOpenTrade(offer.getId());
          if (tradeOptional.isPresent()) {
              trade = tradeOptional.get();

              // verify request is from maker
              if (!sender.equals(request.getMakerNodeAddress())) {
                  log.warn("Trade is already taken"); // TODO (woodser): need to respond with bad ack
                  return;
              }
          } else {

              // verify request is from taker
              if (!sender.equals(request.getTakerNodeAddress())) {
                  log.warn("Ignoring InitTradeRequest from {} with tradeId {} because request must be from taker when trade is not initialized", sender, request.getTradeId());
                  return;
              }

              // compute expected taker fee
              Coin feePerBtc = CoinUtil.getFeePerBtc(FeeService.getTakerFeePerBtc(), Coin.valueOf(offer.getOfferPayload().getAmount()));
              Coin takerFee = CoinUtil.maxCoin(feePerBtc, FeeService.getMinTakerFee());

              // create arbitrator trade
              trade = new ArbitratorTrade(offer,
                      Coin.valueOf(offer.getOfferPayload().getAmount()),
                      takerFee,
                      offer.getOfferPayload().getPrice(),
                      xmrWalletService,
                      getNewProcessModel(offer),
                      UUID.randomUUID().toString(),
                      request.getMakerNodeAddress(),
                      request.getTakerNodeAddress(),
                      request.getArbitratorNodeAddress());

              // set reserve tx hash if available
              Optional<SignedOffer> signedOfferOptional = openOfferManager.getSignedOfferById(request.getTradeId());
              if (signedOfferOptional.isPresent()) {
                  SignedOffer signedOffer = signedOfferOptional.get();
                  trade.getMaker().setReserveTxHash(signedOffer.getReserveTxHash());
              }

              initTradeAndProtocol(trade, getTradeProtocol(trade));
              synchronized (tradableList) {
                  tradableList.add(trade);
              }
          }

          ((ArbitratorProtocol) getTradeProtocol(trade)).handleInitTradeRequest(request, sender, errorMessage -> {
              log.warn("Arbitrator error during trade initialization for trade {}: {}", trade.getId(), errorMessage);
              maybeRemoveTrade(trade);
          });

          requestPersistence();
      }

      // handle request as maker
      else {

          Optional<OpenOffer> openOfferOptional = openOfferManager.getOpenOfferById(request.getTradeId());
          if (!openOfferOptional.isPresent()) {
              return;
          }

          OpenOffer openOffer = openOfferOptional.get();
          if (openOffer.getState() != OpenOffer.State.AVAILABLE) {
              return;
          }

          Offer offer = openOffer.getOffer();

          // verify request is from arbitrator
          Arbitrator arbitrator = user.getAcceptedArbitratorByAddress(sender);
          if (arbitrator == null) {
              log.warn("Ignoring InitTradeRequest from {} with tradeId {} because request is not from accepted arbitrator", sender, request.getTradeId());
              return;
          }

          Optional<Trade> tradeOptional = getOpenTrade(request.getTradeId());
          if (tradeOptional.isPresent()) {
              log.warn("Maker trade already exists with id " + request.getTradeId() + ". This should never happen.");
              return;
          }

          openOfferManager.reserveOpenOffer(openOffer); // TODO (woodser): reserve offer if arbitrator? probably. or, arbitrator does not have open offer?

          Trade trade;
          if (offer.isBuyOffer())
              trade = new BuyerAsMakerTrade(offer,
                      Coin.valueOf(offer.getOfferPayload().getAmount()),
                      Coin.valueOf(offer.getOfferPayload().getMakerFee()), // TODO (woodser): this is maker fee, but Trade calls it taker fee, why not have both?
                      offer.getOfferPayload().getPrice(),
                      xmrWalletService,
                      getNewProcessModel(offer),
                      UUID.randomUUID().toString(),
                      request.getMakerNodeAddress(),
                      request.getTakerNodeAddress(),
                      request.getArbitratorNodeAddress());
          else
              trade = new SellerAsMakerTrade(offer,
                      Coin.valueOf(offer.getOfferPayload().getAmount()),
                      Coin.valueOf(offer.getOfferPayload().getMakerFee()),
                      offer.getOfferPayload().getPrice(),
                      xmrWalletService,
                      getNewProcessModel(offer),
                      UUID.randomUUID().toString(),
                      request.getMakerNodeAddress(),
                      request.getTakerNodeAddress(),
                      request.getArbitratorNodeAddress());

          //System.out.println("TradeManager trade.getTradingPeer().setNodeAddress(): " + sender);
          //trade.getTradingPeer().setNodeAddress(sender);
          // TODO (woodser): what if maker's address changes while offer open, or taker's address changes after multisig deposit available? need to verify and update. see OpenOfferManager.maybeUpdatePersistedOffers()
          trade.getArbitrator().setPubKeyRing(arbitrator.getPubKeyRing());
          trade.getMaker().setPubKeyRing(trade.getOffer().getPubKeyRing());
          initTradeAndProtocol(trade, getTradeProtocol(trade));
          trade.getSelf().setReserveTxHash(openOffer.getReserveTxHash()); // TODO (woodser): initialize in initTradeAndProtocol?
          trade.getSelf().setReserveTxHex(openOffer.getReserveTxHex());
          trade.getSelf().setReserveTxKey(openOffer.getReserveTxKey());
          trade.getSelf().setReserveTxKeyImages(offer.getOfferPayload().getReserveTxKeyImages());
          synchronized (tradableList) {
              tradableList.add(trade);
          }

          // notify on phase changes
          // TODO (woodser): save subscription, bind on startup
          EasyBind.subscribe(trade.statePhaseProperty(), phase -> {
              if (phase == Phase.DEPOSITS_PUBLISHED) {
                  notificationService.sendTradeNotification(trade, "Offer Taken", "Your offer " + offer.getId() + " has been accepted"); // TODO (woodser): use language translation
              }
          });

          ((MakerProtocol) getTradeProtocol(trade)).handleInitTradeRequest(request, sender, errorMessage -> {
              log.warn("Maker error during trade initialization: " + errorMessage);
              openOfferManager.unreserveOpenOffer(openOffer); // offer remains available // TODO: only unreserve if funds not deposited to multisig
              maybeRemoveTrade(trade);
              if (takeOfferRequestErrorMessageHandler != null) takeOfferRequestErrorMessageHandler.handleErrorMessage(errorMessage);
          });

          requestPersistence();
      }
    }

    private void handleInitMultisigRequest(InitMultisigRequest request, NodeAddress peer) {
      log.info("Received InitMultisigRequest from {} with tradeId {} and uid {}", peer, request.getTradeId(), request.getUid());

      try {
          Validator.nonEmptyStringOf(request.getTradeId());
      } catch (Throwable t) {
          log.warn("Invalid InitMultisigRequest " + request.toString());
          return;
      }

      Optional<Trade> tradeOptional = getOpenTrade(request.getTradeId());
      if (!tradeOptional.isPresent()) {
          log.warn("No trade with id " + request.getTradeId());
          return;
      }
      Trade trade = tradeOptional.get();
      getTradeProtocol(trade).handleInitMultisigRequest(request, peer);
    }

    private void handleSignContractRequest(SignContractRequest request, NodeAddress peer) {
        log.info("Received SignContractRequest from {} with tradeId {} and uid {}", peer, request.getTradeId(), request.getUid());

        try {
            Validator.nonEmptyStringOf(request.getTradeId());
        } catch (Throwable t) {
            log.warn("Invalid SignContractRequest message " + request.toString());
            return;
        }

        Optional<Trade> tradeOptional = getOpenTrade(request.getTradeId());
        if (!tradeOptional.isPresent()) {
            log.warn("No trade with id " + request.getTradeId());
            return;
        }
        Trade trade = tradeOptional.get();
        getTradeProtocol(trade).handleSignContractRequest(request, peer);
    }

    private void handleSignContractResponse(SignContractResponse request, NodeAddress peer) {
        log.info("Received SignContractResponse from {} with tradeId {} and uid {}", peer, request.getTradeId(), request.getUid());

        try {
            Validator.nonEmptyStringOf(request.getTradeId());
        } catch (Throwable t) {
            log.warn("Invalid SignContractResponse message " + request.toString());
            return;
        }

        Optional<Trade> tradeOptional = getOpenTrade(request.getTradeId());
        if (!tradeOptional.isPresent()) {
            log.warn("No trade with id " + request.getTradeId());
            return;
        }
        Trade trade = tradeOptional.get();
        ((TraderProtocol) getTradeProtocol(trade)).handleSignContractResponse(request, peer);
    }

    private void handleDepositRequest(DepositRequest request, NodeAddress peer) {
        log.info("Received DepositRequest from {} with tradeId {} and uid {}", peer, request.getTradeId(), request.getUid());

        try {
            Validator.nonEmptyStringOf(request.getTradeId());
        } catch (Throwable t) {
            log.warn("Invalid DepositRequest message " + request.toString());
            return;
        }

        Optional<Trade> tradeOptional = getOpenTrade(request.getTradeId());
        if (!tradeOptional.isPresent()) {
            log.warn("No trade with id " + request.getTradeId());
            return;
        }
        Trade trade = tradeOptional.get();
        ((ArbitratorProtocol) getTradeProtocol(trade)).handleDepositRequest(request, peer);
    }

    private void handleDepositResponse(DepositResponse response, NodeAddress peer) {
        log.info("Received DepositResponse from {} with tradeId {} and uid {}", peer, response.getTradeId(), response.getUid());

        try {
            Validator.nonEmptyStringOf(response.getTradeId());
        } catch (Throwable t) {
            log.warn("Invalid DepositResponse message " + response.toString());
            return;
        }

        Optional<Trade> tradeOptional = getOpenTrade(response.getTradeId());
        if (!tradeOptional.isPresent()) {
            log.warn("No trade with id " + response.getTradeId());
            return;
        }
        Trade trade = tradeOptional.get();
        ((TraderProtocol) getTradeProtocol(trade)).handleDepositResponse(response, peer);
    }

    private void handlePaymentAccountKeyRequest(PaymentAccountKeyRequest request, NodeAddress peer) {
        log.info("Received PaymentAccountKeyRequest from {} with tradeId {} and uid {}", peer, request.getTradeId(), request.getUid());

        try {
            Validator.nonEmptyStringOf(request.getTradeId());
        } catch (Throwable t) {
            log.warn("Invalid PaymentAccountKeyRequest message " + request.toString());
            return;
        }

        Optional<Trade> tradeOptional = getOpenTrade(request.getTradeId());
        if (!tradeOptional.isPresent()) {
            log.warn("No trade with id " + request.getTradeId());
            return;
        }
        Trade trade = tradeOptional.get();
        ((ArbitratorProtocol) getTradeProtocol(trade)).handlePaymentAccountKeyRequest(request, peer);
    }

    private void handleUpdateMultisigRequest(UpdateMultisigRequest request, NodeAddress peer) {
      log.info("Received UpdateMultisigRequest from {} with tradeId {} and uid {}", peer, request.getTradeId(), request.getUid());

      try {
          Validator.nonEmptyStringOf(request.getTradeId());
      } catch (Throwable t) {
          log.warn("Invalid UpdateMultisigRequest message " + request.toString());
          return;
      }

      Optional<Trade> tradeOptional = getOpenTrade(request.getTradeId());
      if (!tradeOptional.isPresent()) throw new RuntimeException("No trade with id " + request.getTradeId()); // TODO (woodser): error handling
      Trade trade = tradeOptional.get();
      getTradeProtocol(trade).handleUpdateMultisigRequest(request, peer, errorMessage -> {
          log.warn("Error handling UpdateMultisigRequest: " + errorMessage);
          if (takeOfferRequestErrorMessageHandler != null)
              takeOfferRequestErrorMessageHandler.handleErrorMessage(errorMessage);
      });
    }

    ///////////////////////////////////////////////////////////////////////////////////////////
    // Take offer
    ///////////////////////////////////////////////////////////////////////////////////////////

    public void checkOfferAvailability(Offer offer,
                                       boolean isTakerApiUser,
                                       String paymentAccountId,
                                       ResultHandler resultHandler,
                                       ErrorMessageHandler errorMessageHandler) {
        offer.checkOfferAvailability(getOfferAvailabilityModel(offer, isTakerApiUser, paymentAccountId), resultHandler, errorMessageHandler);
    }

    // First we check if offer is still available then we create the trade with the protocol
    public void onTakeOffer(Coin amount,
                            Coin txFee,
                            Coin takerFee,
                            Coin fundsNeededForTrade,
                            Offer offer,
                            String paymentAccountId,
                            boolean useSavingsWallet,
                            boolean isTakerApiUser,
                            TradeResultHandler tradeResultHandler,
                            ErrorMessageHandler errorMessageHandler) {

        checkArgument(!wasOfferAlreadyUsedInTrade(offer.getId()));

        OfferAvailabilityModel model = getOfferAvailabilityModel(offer, isTakerApiUser, paymentAccountId);
        offer.checkOfferAvailability(model,
                () -> {
                    if (offer.getState() == Offer.State.AVAILABLE) {
                        Trade trade;
                        if (offer.isBuyOffer()) {
                            trade = new SellerAsTakerTrade(offer,
                                    amount,
                                    takerFee,
                                    model.getTradeRequest().getTradePrice(),
                                    xmrWalletService,
                                    getNewProcessModel(offer),
                                    UUID.randomUUID().toString(),
                                    model.getPeerNodeAddress(),
                                    P2PService.getMyNodeAddress(),
                                    offer.getOfferPayload().getArbitratorSigner());
                        } else {
                            trade = new BuyerAsTakerTrade(offer,
                                    amount,
                                    takerFee,
                                    model.getTradeRequest().getTradePrice(),
                                    xmrWalletService,
                                    getNewProcessModel(offer),
                                    UUID.randomUUID().toString(),
                                    model.getPeerNodeAddress(),
                                    P2PService.getMyNodeAddress(),
                                    offer.getOfferPayload().getArbitratorSigner());
                        }

                        trade.getProcessModel().setTradeMessage(model.getTradeRequest());
                        trade.getProcessModel().setMakerSignature(model.getMakerSignature());
                        trade.getProcessModel().setUseSavingsWallet(useSavingsWallet);
                        trade.getProcessModel().setFundsNeededForTradeAsLong(fundsNeededForTrade.value);
                        trade.getTaker().setPubKeyRing(model.getPubKeyRing());
                        trade.getTaker().setPaymentAccountId(paymentAccountId);

                        TradeProtocol tradeProtocol = TradeProtocolFactory.getNewTradeProtocol(trade);
                        TradeProtocol prev = tradeProtocolByTradeId.put(trade.getUid(), tradeProtocol);
                        if (prev != null) {
                            log.error("We had already an entry with uid {}", trade.getUid());
                        }
                        synchronized (tradableList) {
                            tradableList.add(trade);
                        }

                        initTradeAndProtocol(trade, tradeProtocol);

                        // take offer and persist trade on success
                        ((TakerProtocol) tradeProtocol).onTakeOffer(result -> {
                            tradeResultHandler.handleResult(trade);
                            requestPersistence();
                        }, errorMessage -> {
                            log.warn("Taker error during trade initialization: " + errorMessage);
                            errorMessageHandler.handleErrorMessage(errorMessage);
                            maybeRemoveTrade(trade);
                        });
                        requestPersistence();
                    }
                },
                errorMessage -> {
                    log.warn("Taker error during check offer availability: " + errorMessage);
                    errorMessageHandler.handleErrorMessage(errorMessage);
                });

        requestPersistence();
    }

    private ProcessModel getNewProcessModel(Offer offer) {
        return new ProcessModel(checkNotNull(offer).getId(),
                processModelServiceProvider.getUser().getAccountId(),
                processModelServiceProvider.getKeyRing().getPubKeyRing());
    }

    private OfferAvailabilityModel getOfferAvailabilityModel(Offer offer, boolean isTakerApiUser, String paymentAccountId) {
        return new OfferAvailabilityModel(
                offer,
                keyRing.getPubKeyRing(),
                xmrWalletService,
                p2PService,
                user,
                mediatorManager,
                tradeStatisticsManager,
                isTakerApiUser,
                paymentAccountId,
                offerUtil);
    }


    ///////////////////////////////////////////////////////////////////////////////////////////
    // Complete trade
    ///////////////////////////////////////////////////////////////////////////////////////////

    // TODO (woodser): remove this function
    public void onWithdrawRequest(String toAddress,
          Coin amount,
          Coin fee,
          KeyParameter aesKey,
          Trade trade,
          @Nullable String memo,
          ResultHandler resultHandler,
          FaultHandler faultHandler) {
        throw new RuntimeException("Withdraw trade funds after payout to Haveno wallet not supported");
    }

    // If trade was completed (closed without fault but might be closed by a dispute) we move it to the closed trades
    public void onTradeCompleted(Trade trade) {
        closedTradableManager.add(trade);
        trade.setState(Trade.State.WITHDRAW_COMPLETED);
        maybeRemoveTrade(trade);

        // TODO The address entry should have been removed already. Check and if its the case remove that.
        xmrWalletService.resetAddressEntriesForPendingTrade(trade.getId());
        requestPersistence();
    }


    ///////////////////////////////////////////////////////////////////////////////////////////
    // Dispute
    ///////////////////////////////////////////////////////////////////////////////////////////

    public void closeDisputedTrade(String tradeId, Trade.DisputeState disputeState) {
        Optional<Trade> tradeOptional = getOpenTrade(tradeId);
        if (tradeOptional.isPresent()) {
            Trade trade = tradeOptional.get();
            trade.setDisputeState(disputeState);
            onTradeCompleted(trade);
            xmrWalletService.swapTradeEntryToAvailableEntry(trade.getId(), XmrAddressEntry.Context.TRADE_PAYOUT);
            requestPersistence();
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////
    // Trade period state
    ///////////////////////////////////////////////////////////////////////////////////////////

    public void applyTradePeriodState() {
        updateTradePeriodState();
        clockWatcher.addListener(new ClockWatcher.Listener() {
            @Override
            public void onSecondTick() {
            }

            @Override
            public void onMinuteTick() {
                updateTradePeriodState();
            }
        });
    }

    private void updateTradePeriodState() {
        for (Trade trade : new ArrayList<Trade>(tradableList.getList())) {
            if (!trade.isPayoutPublished()) {
                Date maxTradePeriodDate = trade.getMaxTradePeriodDate();
                Date halfTradePeriodDate = trade.getHalfTradePeriodDate();
                if (maxTradePeriodDate != null && halfTradePeriodDate != null) {
                    Date now = new Date();
                    if (now.after(maxTradePeriodDate)) {
                        trade.setPeriodState(Trade.TradePeriodState.TRADE_PERIOD_OVER);
                        requestPersistence();
                    } else if (now.after(halfTradePeriodDate)) {
                        trade.setPeriodState(Trade.TradePeriodState.SECOND_HALF);
                        requestPersistence();
                    }
                }
            }
        }
    }


    ///////////////////////////////////////////////////////////////////////////////////////////
    // Failed trade handling
    ///////////////////////////////////////////////////////////////////////////////////////////

    // If trade is in already in critical state (if taker role: taker fee; both roles: after deposit published)
    // we move the trade to failedTradesManager
    public void onMoveInvalidTradeToFailedTrades(Trade trade) {
        maybeRemoveTrade(trade);
        failedTradesManager.add(trade);
    }

    public void addFailedTradeToPendingTrades(Trade trade) {
        if (!trade.isInitialized()) {
            initPersistedTrade(trade);
        }
        addTrade(trade);
    }

    public Stream<Trade> getTradesStreamWithFundsLockedIn() {
        return getObservableList().stream().filter(Trade::isFundsLockedIn);
    }

    public Set<String> getSetOfFailedOrClosedTradeIdsFromLockedInFunds() throws TradeTxException {
        AtomicReference<TradeTxException> tradeTxException = new AtomicReference<>();
        Set<String> tradesIdSet = getTradesStreamWithFundsLockedIn()
                .filter(Trade::hasFailed)
                .map(Trade::getId)
                .collect(Collectors.toSet());
        tradesIdSet.addAll(failedTradesManager.getTradesStreamWithFundsLockedIn()
                .filter(trade -> trade.getMakerDepositTx() != null || trade.getTakerDepositTx() != null)
                .map(trade -> {
                    log.warn("We found a failed trade with locked up funds. " +
                            "That should never happen. trade ID=" + trade.getId());
                    return trade.getId();
                })
                .collect(Collectors.toSet()));
        tradesIdSet.addAll(closedTradableManager.getTradesStreamWithFundsLockedIn()
                .map(trade -> {
                  MoneroTx makerDepositTx = trade.getMakerDepositTx();
                  if (makerDepositTx != null) {
                      if (!makerDepositTx.isConfirmed()) {
                        tradeTxException.set(new TradeTxException(Res.get("error.closedTradeWithUnconfirmedDepositTx", trade.getShortId()))); // TODO (woodser): rename to closedTradeWithLockedDepositTx
                      } else {
                        log.warn("We found a closed trade with locked up funds. " +
                                "That should never happen. trade ID=" + trade.getId());
                      }
                  } else {
                      tradeTxException.set(new TradeTxException(Res.get("error.closedTradeWithNoDepositTx", trade.getShortId())));
                  }

                  MoneroTx takerDepositTx = trade.getTakerDepositTx();
                  if (takerDepositTx != null) {
                      if (!takerDepositTx.isConfirmed()) {
                        tradeTxException.set(new TradeTxException(Res.get("error.closedTradeWithUnconfirmedDepositTx", trade.getShortId())));
                      } else {
                        log.warn("We found a closed trade with locked up funds. " +
                                "That should never happen. trade ID=" + trade.getId());
                      }
                  } else {
                      tradeTxException.set(new TradeTxException(Res.get("error.closedTradeWithNoDepositTx", trade.getShortId())));
                  }
                  return trade.getId();
                })
                .collect(Collectors.toSet()));

        if (tradeTxException.get() != null)
            throw tradeTxException.get();

        return tradesIdSet;
    }

    // If trade still has funds locked up it might come back from failed trades
    // Aborts unfailing if the address entries needed are not available
    private boolean unFailTrade(Trade trade) {
        if (!recoverAddresses(trade)) {
            log.warn("Failed to recover address during unFail trade");
            return false;
        }

        initPersistedTrade(trade);

        synchronized (tradableList) {
            if (!tradableList.contains(trade)) {
                tradableList.add(trade);
            }
        }

        return true;
    }

    // The trade is added to pending trades if the associated address entries are AVAILABLE and
    // the relevant entries are changed, otherwise it's not added and no address entries are changed
    private boolean recoverAddresses(Trade trade) {
        // Find addresses associated with this trade.
        var entries = tradeUtil.getAvailableAddresses(trade);
        if (entries == null)
            return false;

        xmrWalletService.recoverAddressEntry(trade.getId(), entries.first,
                XmrAddressEntry.Context.MULTI_SIG);
        xmrWalletService.recoverAddressEntry(trade.getId(), entries.second,
                XmrAddressEntry.Context.TRADE_PAYOUT);
        return true;
    }


    ///////////////////////////////////////////////////////////////////////////////////////////
    // Getters, Utils
    ///////////////////////////////////////////////////////////////////////////////////////////

    public ObservableList<Trade> getObservableList() {
        synchronized (tradableList) {
            return tradableList.getObservableList();
        }
    }

    public BooleanProperty persistedTradesInitializedProperty() {
        return persistedTradesInitialized;
    }

    public boolean isMyOffer(Offer offer) {
        return offer.isMyOffer(keyRing);
    }

    public boolean wasOfferAlreadyUsedInTrade(String offerId) {
        return getOpenTrade(offerId).isPresent() ||
                failedTradesManager.getTradeById(offerId).isPresent() ||
                closedTradableManager.getTradableById(offerId).isPresent();
    }

    public boolean isBuyer(Offer offer) {
        // If I am the maker, we use the OfferDirection, otherwise the mirrored direction
        if (isMyOffer(offer))
            return offer.isBuyOffer();
        else
            return offer.getDirection() == OfferDirection.SELL;
    }

    // TODO (woodser): make Optional<Trade> versus Trade return types consistent
    public Trade getTrade(String tradeId) {
        return getOpenTrade(tradeId).orElseGet(() -> getClosedTrade(tradeId).orElseGet(() -> null));
    }

    public Optional<Trade> getOpenTrade(String tradeId) {
        return tradableList.stream().filter(e -> e.getId().equals(tradeId)).findFirst();
    }

    public List<Trade> getOpenTrades() {
        return ImmutableList.copyOf(getObservableList().stream()
                .filter(e -> e instanceof Trade)
                .map(e -> e)
                .collect(Collectors.toList()));
    }

    public Optional<Trade> getClosedTrade(String tradeId) {
        return closedTradableManager.getClosedTrades().stream().filter(e -> e.getId().equals(tradeId)).findFirst();
    }

    private synchronized void maybeRemoveTrade(Trade trade) {
        log.info("TradeManager.maybeRemoveTrade()");
        synchronized(tradableList) {
            if (!tradableList.contains(trade)) return;

            // delete trade if not possibly funded
            if (trade.getPhase().ordinal() < Trade.Phase.DEPOSIT_REQUESTED.ordinal() || trade.getPhase().ordinal() >= Trade.Phase.PAYOUT_PUBLISHED.ordinal()) { // TODO: delete after payout unlocked

                // remove trade
                tradableList.remove(trade);

                // unreserve trade key images
                if (trade instanceof TakerTrade && trade.getSelf().getReserveTxKeyImages() != null) {
                    for (String keyImage : trade.getSelf().getReserveTxKeyImages()) {
                        xmrWalletService.getWallet().thawOutput(keyImage);
                    }
                }

                // delete multisig wallet
                deleteTradeWallet(trade);

                // unregister and persist
                p2PService.removeDecryptedDirectMessageListener(getTradeProtocol(trade));
                requestPersistence();
            } else {
                log.warn("Not deleting trade " + trade.getId() + " because its trade wallet might be funded");
                // TODO: schedule wallet for deletion after unlock
            }
        }
    }

    private void addTrade(Trade trade) {
        synchronized(tradableList) {
            if (tradableList.add(trade)) {
                requestPersistence();
            }
        }
    }

    // TODO Remove once tradableList is refactored to a final field
    //  (part of the persistence refactor PR)
    private void onTradesChanged() {
        this.numPendingTrades.set(getObservableList().size());
    }

    private void deleteTradeWallet(Trade trade) {
        if (xmrWalletService.multisigWalletExists(trade.getId())) xmrWalletService.deleteMultisigWallet(trade.getId());
        else log.warn("Multisig wallet to delete for trade {} does not exist", trade.getId());
    }
}
