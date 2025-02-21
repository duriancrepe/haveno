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

package bisq.core.trade.protocol.tasks.mediation;

import bisq.core.trade.Trade;
import bisq.core.trade.protocol.tasks.TradeTask;

import bisq.common.taskrunner.TaskRunner;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FinalizeMediatedPayoutTx extends TradeTask {

    public FinalizeMediatedPayoutTx(TaskRunner<Trade> taskHandler, Trade trade) {
        super(taskHandler, trade);
    }

    @Override
    protected void run() {
        try {
            runInterceptHook();
            throw new RuntimeException("FinalizeMediatedPayoutTx not implemented for xmr");

//            Transaction depositTx = checkNotNull(trade.getDepositTx());
//            String tradeId = trade.getId();
//            TradingPeer tradingPeer = trade.getTradingPeer();
//            BtcWalletService walletService = processModel.getBtcWalletService();
//            Offer offer = checkNotNull(trade.getOffer(), "offer must not be null");
//            Coin tradeAmount = checkNotNull(trade.getTradeAmount(), "tradeAmount must not be null");
//            Contract contract = checkNotNull(trade.getContract(), "contract must not be null");
//
//            checkNotNull(trade.getTradeAmount(), "trade.getTradeAmount() must not be null");
//
//
//            byte[] mySignature = checkNotNull(processModel.getMediatedPayoutTxSignature(),
//                    "processModel.getTxSignatureFromMediation must not be null");
//            byte[] peersSignature = checkNotNull(tradingPeer.getMediatedPayoutTxSignature(),
//                    "tradingPeer.getTxSignatureFromMediation must not be null");
//
//            boolean isMyRoleBuyer = contract.isMyRoleBuyer(processModel.getPubKeyRing());
//            byte[] buyerSignature = isMyRoleBuyer ? mySignature : peersSignature;
//            byte[] sellerSignature = isMyRoleBuyer ? peersSignature : mySignature;
//
//            Coin totalPayoutAmount = offer.getBuyerSecurityDeposit().add(tradeAmount).add(offer.getSellerSecurityDeposit());
//            Coin buyerPayoutAmount = Coin.valueOf(processModel.getBuyerPayoutAmountFromMediation());
//            Coin sellerPayoutAmount = Coin.valueOf(processModel.getSellerPayoutAmountFromMediation());
//            checkArgument(totalPayoutAmount.equals(buyerPayoutAmount.add(sellerPayoutAmount)),
//                    "Payout amount does not match buyerPayoutAmount=" + buyerPayoutAmount.toFriendlyString() +
//                            "; sellerPayoutAmount=" + sellerPayoutAmount);
//
//            String myPayoutAddressString = walletService.getOrCreateAddressEntry(tradeId, AddressEntry.Context.TRADE_PAYOUT).getAddressString();
//            String peersPayoutAddressString = tradingPeer.getPayoutAddressString();
//            String buyerPayoutAddressString = isMyRoleBuyer ? myPayoutAddressString : peersPayoutAddressString;
//            String sellerPayoutAddressString = isMyRoleBuyer ? peersPayoutAddressString : myPayoutAddressString;
//
//            byte[] myMultiSigPubKey = processModel.getMyMultiSigPubKey();
//            byte[] peersMultiSigPubKey = tradingPeer.getMultiSigPubKey();
//            byte[] buyerMultiSigPubKey = isMyRoleBuyer ? myMultiSigPubKey : peersMultiSigPubKey;
//            byte[] sellerMultiSigPubKey = isMyRoleBuyer ? peersMultiSigPubKey : myMultiSigPubKey;
//
//            DeterministicKey multiSigKeyPair = walletService.getMultiSigKeyPair(tradeId, myMultiSigPubKey);
//
//            checkArgument(Arrays.equals(myMultiSigPubKey,
//                    walletService.getOrCreateAddressEntry(tradeId, AddressEntry.Context.MULTI_SIG).getPubKey()),
//                    "myMultiSigPubKey from AddressEntry must match the one from the trade data. trade id =" + tradeId);
//
//            Transaction transaction = processModel.getTradeWalletService().finalizeMediatedPayoutTx(
//                    depositTx,
//                    buyerSignature,
//                    sellerSignature,
//                    buyerPayoutAmount,
//                    sellerPayoutAmount,
//                    buyerPayoutAddressString,
//                    sellerPayoutAddressString,
//                    multiSigKeyPair,
//                    buyerMultiSigPubKey,
//                    sellerMultiSigPubKey
//            );
//
//            trade.setPayoutTx(transaction);
//
//            processModel.getTradeManager().requestPersistence();
//
//            walletService.resetCoinLockedInMultiSigAddressEntry(tradeId);
//
//            complete();
        } catch (Throwable t) {
            failed(t);
        }
    }
}

