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

package bisq.core.support.dispute.arbitration.arbitrator;

import bisq.core.support.dispute.agent.DisputeAgent;

import bisq.network.p2p.NodeAddress;

import bisq.common.crypto.PubKeyRing;
import bisq.common.proto.ProtoUtil;
import bisq.common.util.CollectionUtils;

import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

@EqualsAndHashCode(callSuper = true)
@Slf4j
@Getter
public final class Arbitrator extends DisputeAgent {
    private final String xmrAddress;

    public Arbitrator(NodeAddress nodeAddress,
                      String xmrAddress,
                      PubKeyRing pubKeyRing,
                      List<String> languageCodes,
                      long registrationDate,
                      byte[] registrationPubKey,
                      String registrationSignature,
                      @Nullable String emailAddress,
                      @Nullable String info,
                      @Nullable Map<String, String> extraDataMap) {

        super(nodeAddress,
                pubKeyRing,
                languageCodes,
                registrationDate,
                registrationPubKey,
                registrationSignature,
                emailAddress,
                info,
                extraDataMap);

        this.xmrAddress = xmrAddress;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////
    // PROTO BUFFER
    ///////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public protobuf.StoragePayload toProtoMessage() {
        protobuf.Arbitrator.Builder builder = protobuf.Arbitrator.newBuilder()
                .setNodeAddress(nodeAddress.toProtoMessage())
                .setXmrAddress(xmrAddress)
                .setPubKeyRing(pubKeyRing.toProtoMessage())
                .addAllLanguageCodes(languageCodes)
                .setRegistrationDate(registrationDate)
                .setRegistrationPubKey(ByteString.copyFrom(registrationPubKey))
                .setRegistrationSignature(registrationSignature);
        Optional.ofNullable(emailAddress).ifPresent(builder::setEmailAddress);
        Optional.ofNullable(info).ifPresent(builder::setInfo);
        Optional.ofNullable(extraDataMap).ifPresent(builder::putAllExtraData);
        return protobuf.StoragePayload.newBuilder().setArbitrator(builder).build();
    }

    public static Arbitrator fromProto(protobuf.Arbitrator proto) {
        return new Arbitrator(NodeAddress.fromProto(proto.getNodeAddress()),
                proto.getXmrAddress(),
                PubKeyRing.fromProto(proto.getPubKeyRing()),
                new ArrayList<>(proto.getLanguageCodesList()),
                proto.getRegistrationDate(),
                proto.getRegistrationPubKey().toByteArray(),
                proto.getRegistrationSignature(),
                ProtoUtil.stringOrNullFromProto(proto.getEmailAddress()),
                ProtoUtil.stringOrNullFromProto(proto.getInfo()),
                CollectionUtils.isEmpty(proto.getExtraDataMap()) ? null : proto.getExtraDataMap());
    }


    ///////////////////////////////////////////////////////////////////////////////////////////
    // API
    ///////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public String toString() {
        return "Arbitrator{" +
                ",\n     xmrAddress='" + xmrAddress + '\'' +
                "\n} " + super.toString();
    }
}
