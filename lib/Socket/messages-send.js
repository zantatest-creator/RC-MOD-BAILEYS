import NodeCache from '@cacheable/node-cache';
import { Boom } from '@hapi/boom';
import { proto } from '../../WAProto/index.js';
import { DEFAULT_CACHE_TTLS, WA_DEFAULT_EPHEMERAL } from '../Defaults/index.js';
import {
    aggregateMessageKeysNotFromMe,
    assertMediaContent,
    bindWaitForEvent,
    decryptMediaRetryData,
    encodeNewsletterMessage,
    encodeSignedDeviceIdentity,
    encodeWAMessage,
    encryptMediaRetryRequest,
    extractDeviceJids,
    generateMessageIDV2,
    generateWAMessage,
    getStatusCodeForMediaRetry,
    getUrlFromDirectPath,
    getWAUploadToServer,
    normalizeMessageContent,
    parseAndInjectE2ESessions,
    unixTimestampSeconds
} from '../Utils/index.js';
import { getUrlInfo } from '../Utils/link-preview.js';
import {
    getBinaryNodeChild,
    getBinaryNodeChildren,
    isJidGroup,
    areJidsSameUser,
    jidDecode,
    jidEncode,
    jidNormalizedUser,
    S_WHATSAPP_NET,
    STORIES_JID
} from '../WABinary/index.js';
import { USyncQuery, USyncUser } from '../WAUSync/index.js';
import { makeGroupsSocket } from './groups.js';
import { makeNewsletterSocket } from './newsletter.js';
import GrpStatus from './gcstatus.js';

// Local helper to check if a JID is a regular user (not group, not lid, etc.)
const isJidUser = (jid) => {
    const { server } = jidDecode(jid);
    return server === 's.whatsapp.net';
};

// Permanently blacklist device JIDs that WhatsApp rejects with not-acceptable.
const _deadDeviceJids = new Set();

export const makeMessagesSocket = (config) => {
    const {
        logger,
        linkPreviewImageThumbnailWidth,
        generateHighQualityLinkPreview,
        options: axiosOptions,
        patchMessageBeforeSending,
        cachedGroupMetadata,
    } = config;
    const sock = makeNewsletterSocket(
        makeGroupsSocket(config),
    );
    const {
        ev,
        authState,
        messageMutex,          // <-- changed from processingMutex to messageMutex
        signalRepository,
        upsertMessage,
        query,
        fetchPrivacySettings,
        sendNode,
        groupMetadata,
        groupToggleEphemeral,
    } = sock;
    const userDevicesCache =
        config.userDevicesCache ||
        new NodeCache({
            stdTTL: DEFAULT_CACHE_TTLS.USER_DEVICES,
            useClones: false,
        });
    let mediaConn;

    const refreshMediaConn = async (forceGet = false) => {
        const media = await mediaConn;
        if (
            !media ||
            forceGet ||
            new Date().getTime() - media.fetchDate.getTime() > media.ttl * 1000
        ) {
            mediaConn = (async () => {
                const result = await query({
                    tag: "iq",
                    attrs: {
                        type: "set",
                        xmlns: "w:m",
                        to: S_WHATSAPP_NET,
                    },
                    content: [{ tag: "media_conn", attrs: {} }],
                });
                const mediaConnNode = getBinaryNodeChild(
                    result,
                    "media_conn",
                );
                const node = {
                    hosts: getBinaryNodeChildren(
                        mediaConnNode,
                        "host",
                    ).map(({ attrs }) => ({
                        hostname: attrs.hostname,
                        maxContentLengthBytes: +attrs.maxContentLengthBytes,
                    })),
                    auth: mediaConnNode.attrs.auth,
                    ttl: +mediaConnNode.attrs.ttl,
                    fetchDate: new Date(),
                };
                logger.debug("fetched media conn");
                return node;
            })();
        }
        return mediaConn;
    };

    const sendReceipt = async (jid, participant, messageIds, type) => {
        const node = {
            tag: "receipt",
            attrs: {
                id: messageIds[0],
            },
        };
        const isReadReceipt = type === "read" || type === "read-self";
        if (isReadReceipt) {
            node.attrs.t = unixTimestampSeconds().toString();
        }
        if (type === "sender" && isJidUser(jid)) {
            node.attrs.recipient = jid;
            node.attrs.to = participant;
        } else {
            node.attrs.to = jid;
            if (participant) {
                node.attrs.participant = participant;
            }
        }
        if (type) {
            node.attrs.type = type;
        }
        const remainingMessageIds = messageIds.slice(1);
        if (remainingMessageIds.length) {
            node.content = [
                {
                    tag: "list",
                    attrs: {},
                    content: remainingMessageIds.map((id) => ({
                        tag: "item",
                        attrs: { id },
                    })),
                },
            ];
        }
        logger.debug(
            { attrs: node.attrs, messageIds },
            "sending receipt for messages",
        );
        await sendNode(node);
    };

    const sendReceipts = async (keys, type) => {
        const recps = aggregateMessageKeysNotFromMe(keys);
        for (const { jid, participant, messageIds } of recps) {
            await sendReceipt(jid, participant, messageIds, type);
        }
    };

    const readMessages = async (keys) => {
        const privacySettings = await fetchPrivacySettings();
        const readType =
            privacySettings.readreceipts === "all" ? "read" : "read-self";
        await sendReceipts(keys, readType);
    };

    const getUSyncDevices = async (jids, useCache, ignoreZeroDevices) => {
        const deviceResults = [];
        if (!useCache) {
            logger.debug("not using cache for devices");
        }
        const toFetch = [];
        jids = Array.from(new Set(jids));
        for (let jid of jids) {
            const user = jidDecode(jid)?.user;
            jid = jidNormalizedUser(jid);
            if (useCache) {
                const devices = userDevicesCache.get(user);
                if (devices) {
                    deviceResults.push(...devices);
                    logger.trace({ user }, "using cache for devices");
                } else {
                    toFetch.push(jid);
                }
            } else {
                toFetch.push(jid);
            }
        }
        if (!toFetch.length) {
            return deviceResults;
        }
        const query = new USyncQuery()
            .withContext("message")
            .withDeviceProtocol();
        for (const jid of toFetch) {
            query.withUser(new USyncUser().withId(jid));
        }
        const result = await sock.executeUSyncQuery(query);
        if (result) {
            const extracted = extractDeviceJids(
                result?.list,
                authState.creds.me.id,
                ignoreZeroDevices,
            );
            const deviceMap = {};
            for (const item of extracted) {
                deviceMap[item.user] = deviceMap[item.user] || [];
                deviceMap[item.user].push(item);
                deviceResults.push(item);
            }
            for (const key in deviceMap) {
                userDevicesCache.set(key, deviceMap[key]);
            }
        }
        return deviceResults;
    };

    const assertSessions = async (jids, force) => {
        let didFetchNewSession = false;
        let jidsRequiringFetch = [];
        const liveJids = jids.filter(j => !_deadDeviceJids.has(j));
        if (force) {
            jidsRequiringFetch = liveJids;
        } else {
            const addrs = liveJids.map((jid) =>
                signalRepository.jidToSignalProtocolAddress(jid),
            );
            const sessions = await authState.keys.get("session", addrs);
            for (const jid of liveJids) {
                const signalId =
                    signalRepository.jidToSignalProtocolAddress(jid);
                if (!sessions[signalId]) {
                    jidsRequiringFetch.push(jid);
                }
            }
        }
        if (jidsRequiringFetch.length) {
            logger.debug({ jidsRequiringFetch }, "fetching sessions");
            const doPreKeyIQ = async (jidList) => {
                return query({
                    tag: "iq",
                    attrs: {
                        xmlns: "encrypt",
                        type: "get",
                        to: S_WHATSAPP_NET,
                    },
                    content: [
                        {
                            tag: "key",
                            attrs: {},
                            content: jidList.map((jid) => ({
                                tag: "user",
                                attrs: { jid },
                            })),
                        },
                    ],
                });
            };
            let batchOk = false;
            if (jidsRequiringFetch.length > 1) {
                try {
                    const result = await doPreKeyIQ(jidsRequiringFetch);
                    await parseAndInjectE2ESessions(result, signalRepository);
                    didFetchNewSession = true;
                    batchOk = true;
                } catch (batchErr) {
                    logger.debug({ batchErr: batchErr.message }, "batch prekey IQ failed, retrying per-JID");
                }
            }
            if (!batchOk) {
                for (const jid of jidsRequiringFetch) {
                    try {
                        const result = await doPreKeyIQ([jid]);
                        await parseAndInjectE2ESessions(result, signalRepository);
                        didFetchNewSession = true;
                    } catch (singleErr) {
                        _deadDeviceJids.add(jid);
                        logger.debug({ jid, err: singleErr.message }, "blacklisting dead device");
                    }
                }
            }
        }
        return didFetchNewSession;
    };

    const sendPeerDataOperationMessage = async (pdoMessage) => {
        if (!authState.creds.me?.id) {
            throw new Boom("Not authenticated");
        }
        const protocolMessage = {
            protocolMessage: {
                peerDataOperationRequestMessage: pdoMessage,
                type: proto.Message.ProtocolMessage.Type.PEER_DATA_OPERATION_REQUEST_MESSAGE,
            },
        };
        const meJid = jidNormalizedUser(authState.creds.me.id);
        const msgId = await relayMessage(meJid, protocolMessage, {
            additionalAttributes: {
                category: "peer",
                push_priority: "high_force",
            },
        });
        return msgId;
    };

    const createParticipantNodes = async (jids, message, extraAttrs) => {
        let patched = await patchMessageBeforeSending(message, jids);
        if (!Array.isArray(patched)) {
            patched = jids
                ? jids.map((jid) => ({ recipientJid: jid, ...patched }))
                : [patched];
        }
        let shouldIncludeDeviceIdentity = false;
        const nodes = await Promise.all(
            patched.map(async (patchedMessageWithJid) => {
                const { recipientJid: jid, ...patchedMessage } = patchedMessageWithJid;
                if (!jid) {
                    return {};
                }
                const bytes = encodeWAMessage(patchedMessage);
                const { type, ciphertext } = await signalRepository.encryptMessage({ jid, data: bytes });
                if (type === "pkmsg") {
                    shouldIncludeDeviceIdentity = true;
                }
                const node = {
                    tag: "to",
                    attrs: { jid },
                    content: [
                        {
                            tag: "enc",
                            attrs: {
                                v: "2",
                                type,
                                ...(extraAttrs || {}),
                            },
                            content: ciphertext,
                        },
                    ],
                };
                return node;
            }),
        );
        return { nodes, shouldIncludeDeviceIdentity };
    };

    // Helper to check if message has buttons
    const hasButtonContent = (message) => {
        const content = normalizeMessageContent(message);
        if (!content) return false;
        return !!(content.buttonsMessage || 
                  content.buttonsResponseMessage || 
                  content.listMessage ||
                  content.listResponseMessage ||
                  content.interactiveMessage ||
                  content.interactiveResponseMessage ||
                  content.nativeFlowMessage ||
                  content.templateMessage);
    };

    // Helper to get biz node for buttons
    const getBizNodeForButtons = (message) => {
        const content = normalizeMessageContent(message);
        if (!content) return null;
        
        if (content.buttonsMessage || content.interactiveMessage || content.nativeFlowMessage) {
            return {
                tag: "biz",
                attrs: {},
                content: [{
                    tag: "interactive",
                    attrs: {
                        type: "native_flow",
                        v: "1"
                    },
                    content: [{
                        tag: "native_flow",
                        attrs: { v: "9", name: "mixed" }
                    }]
                }]
            };
        }
        
        if (content.listMessage) {
            return {
                tag: "biz",
                attrs: {},
                content: [{
                    tag: "list",
                    attrs: {
                        type: "product_list",
                        v: "2"
                    }
                }]
            };
        }
        
        return null;
    };

    // Helper to check if message is a group status
    const isGroupStatusMessage = (message) => {
        const content = normalizeMessageContent(message);
        if (!content) return false;
        return !!(content.groupStatusMessage || content.groupStatusMessageV2);
    };

    // CRITICAL FIX: getMessageType should check normalized content
    const getMessageType = (message) => {
        const normalized = normalizeMessageContent(message);
        
        if (normalized?.groupStatusMessage) return "text";
        if (normalized?.groupStatusMessageV2) return "text";
        
        if (normalized?.pollCreationMessage ||
            normalized?.pollCreationMessageV2 ||
            normalized?.pollCreationMessageV3) {
            return "poll";
        }
        
        const mediaType = getMediaTypeFromNormalized(normalized);
        if (mediaType) {
            return "text";
        }
        
        return "text";
    };

    // CRITICAL FIX: Media type detection from normalized content
    const getMediaTypeFromNormalized = (normalized) => {
        if (!normalized) return;
        
        if (normalized.imageMessage) return "image";
        if (normalized.videoMessage) return normalized.videoMessage.gifPlayback ? "gif" : "video";
        if (normalized.audioMessage) return normalized.audioMessage.ptt ? "ptt" : "audio";
        if (normalized.contactMessage) return "vcard";
        if (normalized.documentMessage) return "document";
        if (normalized.contactsArrayMessage) return "contact_array";
        if (normalized.liveLocationMessage) return "livelocation";
        if (normalized.stickerMessage) return "sticker";
        if (normalized.orderMessage) return "order";
        if (normalized.productMessage) return "product";
        if (normalized.groupInviteMessage) return "url";
        
        if (normalized.groupStatusMessageV2) {
            const innerMsg = normalized.groupStatusMessageV2.message || {};
            if (innerMsg.imageMessage) return "image";
            if (innerMsg.videoMessage) return innerMsg.videoMessage.gifPlayback ? "gif" : "video";
            if (innerMsg.audioMessage) return innerMsg.audioMessage.ptt ? "ptt" : "audio";
            if (innerMsg.stickerMessage) return "sticker";
        }
        
        return;
    };

    // Keep original getMediaType for backward compatibility
    const getMediaType = (message) => {
        const normalized = normalizeMessageContent(message);
        return getMediaTypeFromNormalized(normalized);
    };

    const relayMessage = async (
        jid,
        message,
        {
            messageId: msgId,
            participant,
            additionalAttributes,
            additionalNodes,
            useUserDevicesCache,
            useCachedGroupMetadata,
            statusJidList,
        },
    ) => {
        const meId = authState.creds.me.id;
        let shouldIncludeDeviceIdentity = false;
        const { user, server } = jidDecode(jid);
        const statusJid = "status@broadcast";
        const isGroup = server === "g.us";
        const isStatus = jid === statusJid;
        const isLid = server === "lid";
        const isNewsletter = server === "newsletter";
        msgId = msgId || generateMessageIDV2(sock.user?.id);
        useUserDevicesCache = useUserDevicesCache !== false;
        useCachedGroupMetadata = useCachedGroupMetadata !== false && !isStatus;
        const participants = [];
        const destinationJid = !isStatus
            ? jidEncode(user, isLid ? "lid" : isGroup ? "g.us" : "s.whatsapp.net")
            : statusJid;
        const binaryNodeContent = [];
        const devices = [];
        const meMsg = {
            deviceSentMessage: {
                destinationJid,
                message,
            },
            messageContextInfo: message.messageContextInfo,
        };
        const extraAttrs = {};
        
        if (participant) {
            if (!isGroup && !isStatus) {
                additionalAttributes = {
                    ...additionalAttributes,
                    device_fanout: "false",
                };
            }
            const { user, device } = jidDecode(participant.jid);
            devices.push({ user, device });
        }
        
        await authState.keys.transaction(async () => {
            const mediaType = getMediaType(message);
            if (mediaType) {
                extraAttrs["mediatype"] = mediaType;
            }
            
            if (isNewsletter) {
                const patched = patchMessageBeforeSending
                    ? await patchMessageBeforeSending(message, [])
                    : message;
                const bytes = encodeNewsletterMessage(patched);
                binaryNodeContent.push({
                    tag: "plaintext",
                    attrs: {},
                    content: bytes,
                });
                const stanza = {
                    tag: "message",
                    attrs: {
                        to: jid,
                        id: msgId,
                        type: getMessageType(message),
                        ...(additionalAttributes || {}),
                    },
                    content: binaryNodeContent,
                };
                logger.debug({ msgId }, `sending newsletter message to ${jid}`);
                await sendNode(stanza);
                return;
            }
            
            if (normalizeMessageContent(message)?.pinInChatMessage) {
                extraAttrs["decrypt-fail"] = "hide";
            }
            
            if (isGroup || isStatus) {
                const [groupData, senderKeyMap] = await Promise.all([
                    (async () => {
                        let groupData = useCachedGroupMetadata && cachedGroupMetadata
                            ? await cachedGroupMetadata(jid)
                            : undefined;
                        if (
                            groupData &&
                            Array.isArray(groupData?.participants)
                        ) {
                            logger.trace(
                                {
                                    jid,
                                    participants: groupData.participants.length,
                                },
                                "using cached group metadata",
                            );
                        } else if (!isStatus) {
                            groupData = await groupMetadata(jid);
                        }
                        return groupData;
                    })(),
                    (async () => {
                        if (!participant && !isStatus) {
                            const result = await authState.keys.get("sender-key-memory", [jid]);
                            return result[jid] || {};
                        }
                        return {};
                    })(),
                ]);
                
                if (!participant) {
                    const participantsList = groupData && !isStatus
                        ? groupData.participants.map((p) => p.id)
                        : [];
                    if (isStatus) {
                        const normalizedMeId = jidNormalizedUser(meId);
                        if (!participantsList.includes(normalizedMeId)) {
                            participantsList.push(normalizedMeId);
                        }
                        const meLid = authState.creds.me?.lid;
                        if (meLid) {
                            const normalizedMeLid = jidNormalizedUser(meLid);
                            if (!participantsList.includes(normalizedMeLid)) {
                                participantsList.push(normalizedMeLid);
                            }
                        }
                        if (statusJidList && Array.isArray(statusJidList) && statusJidList.length > 0) {
                            for (const jidItem of statusJidList) {
                                const normalizedJid = jidNormalizedUser(jidItem);
                                if (!participantsList.includes(normalizedJid)) {
                                    participantsList.push(normalizedJid);
                                }
                            }
                        } else {
                            logger.warn({ meId }, 'No statusJidList provided for status message');
                        }
                        logger.debug({ participantsList, statusJidList }, 'Status message participants');
                    }
                    
                    if (!isStatus) {
                        additionalAttributes = {
                            ...additionalAttributes,
                            addressing_mode: groupData?.addressingMode || "pn",
                        };
                    }
                    
                    const additionalDevices = await getUSyncDevices(participantsList, !!useUserDevicesCache, false);
                    devices.push(...additionalDevices);
                }
                
                const patched = await patchMessageBeforeSending(message);
                if (Array.isArray(patched)) {
                    throw new Boom("Per-jid patching is not supported in groups");
                }
                
                const bytes = encodeWAMessage(patched);
                const { ciphertext, senderKeyDistributionMessage } = await signalRepository.encryptGroupMessage({
                    group: destinationJid,
                    data: bytes,
                    meId,
                });
                
                const senderKeyJids = [];
                for (const { user, device } of devices) {
                    const jid = jidEncode(user, groupData?.addressingMode === "lid" ? "lid" : "s.whatsapp.net", device);
                    if (!senderKeyMap[jid] || !!participant) {
                        if (_deadDeviceJids.has(jid)) {
                            senderKeyMap[jid] = true;
                        } else {
                            senderKeyJids.push(jid);
                            senderKeyMap[jid] = true;
                        }
                    }
                }
                
                if (senderKeyJids.length) {
                    logger.debug({ senderKeyJids }, "sending new sender key");
                    const senderKeyMsg = {
                        senderKeyDistributionMessage: {
                            axolotlSenderKeyDistributionMessage: senderKeyDistributionMessage,
                            groupId: destinationJid,
                        },
                    };
                    await assertSessions(senderKeyJids, false);
                    
                    const sessionAddrs = senderKeyJids.map(sjid => signalRepository.jidToSignalProtocolAddress(sjid));
                    const sessCheck = await authState.keys.get('session', sessionAddrs);
                    const validSenderKeyJids = senderKeyJids.filter((sjid, i) => !!sessCheck[sessionAddrs[i]]);
                    
                    const result = await createParticipantNodes(validSenderKeyJids, senderKeyMsg, extraAttrs);
                    shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || result.shouldIncludeDeviceIdentity;
                    participants.push(...result.nodes);
                }
                
                binaryNodeContent.push({
                    tag: "enc",
                    attrs: { v: "2", type: "skmsg" },
                    content: ciphertext,
                });
                
                await authState.keys.set({
                    "sender-key-memory": { [jid]: senderKeyMap },
                });
            } else {
                const { user: meUser } = jidDecode(meId);
                const meLid = authState.creds.me?.lid;
                const meLidUser = meLid?.split(":")[0];
                const mePhone = meUser?.split(":")[0];

                if (!participant) {
                    devices.push({ user });
                    const isSelfMessage = user === meUser || user === mePhone || user === meLidUser;
                    if (!isSelfMessage) {
                        devices.push({ user: meUser });
                    }
                    if (additionalAttributes?.category !== "peer") {
                        const targetJid = isLid ? jid : jidNormalizedUser(jid);
                        const additionalDevices = await getUSyncDevices([meId, targetJid], !!useUserDevicesCache, true);
                        devices.push(...additionalDevices);
                    }
                }
                
                const allJids = [];
                const meJids = [];
                const otherJids = [];
                for (const { user: deviceUser, device } of devices) {
                    const isMe = deviceUser === meUser || deviceUser === mePhone || deviceUser === meLidUser;
                    let encodedJid;

                    if (isMe) {
                        encodedJid = jidEncode(isLid ? meLidUser || mePhone : mePhone, isLid ? "lid" : "s.whatsapp.net", device);
                    } else {
                        encodedJid = jidEncode(deviceUser, isLid ? "lid" : "s.whatsapp.net", device);
                    }

                    if (isMe) {
                        meJids.push(encodedJid);
                    } else {
                        otherJids.push(encodedJid);
                    }
                    allJids.push(encodedJid);
                }
                
                await assertSessions(allJids, false);
                
                const [
                    { nodes: meNodes, shouldIncludeDeviceIdentity: s1 },
                    { nodes: otherNodes, shouldIncludeDeviceIdentity: s2 },
                ] = await Promise.all([
                    createParticipantNodes(meJids, meMsg, extraAttrs),
                    createParticipantNodes(otherJids, message, extraAttrs),
                ]);
                
                participants.push(...meNodes);
                participants.push(...otherNodes);
                shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || s1 || s2;
            }
            
            if (participants.length) {
                if (additionalAttributes?.category === "peer") {
                    const peerNode = participants[0]?.content?.[0];
                    if (peerNode) {
                        binaryNodeContent.push(peerNode);
                    }
                } else {
                    binaryNodeContent.push({
                        tag: "participants",
                        attrs: {},
                        content: participants,
                    });
                }
            }
            
            const normalizedMsg = normalizeMessageContent(message);
            const isMediaMessage = !!(normalizedMsg?.imageMessage || 
                                       normalizedMsg?.videoMessage || 
                                       normalizedMsg?.audioMessage || 
                                       normalizedMsg?.documentMessage || 
                                       normalizedMsg?.stickerMessage);
            const isButtonMessage = hasButtonContent(message);
            
            if (!isNewsletter && !isGroupStatusMessage(message) && isButtonMessage && !isMediaMessage) {
                const bizNode = getBizNodeForButtons(message);
                if (bizNode) {
                    binaryNodeContent.push(bizNode);
                    logger.debug({ jid }, "adding biz node for standalone buttons");
                }
            }
            
            const stanza = {
                tag: "message",
                attrs: {
                    id: msgId,
                    type: getMessageType(message),
                    ...(additionalAttributes || {}),
                },
                content: binaryNodeContent,
            };
            
            if (participant) {
                if (isJidGroup(destinationJid)) {
                    stanza.attrs.to = destinationJid;
                    stanza.attrs.participant = participant.jid;
                } else if (areJidsSameUser(participant.jid, meId)) {
                    stanza.attrs.to = participant.jid;
                    stanza.attrs.recipient = destinationJid;
                } else {
                    stanza.attrs.to = participant.jid;
                }
            } else {
                stanza.attrs.to = destinationJid;
            }
            
            if (shouldIncludeDeviceIdentity) {
                stanza.content.push({
                    tag: "device-identity",
                    attrs: {},
                    content: encodeSignedDeviceIdentity(authState.creds.account, true),
                });
                logger.debug({ jid }, "adding device identity");
            }
            
            if (additionalNodes && additionalNodes.length > 0) {
                stanza.content.push(...additionalNodes);
            }
            
            logger.debug({ msgId }, `sending message to ${participants.length} devices`);
            await sendNode(stanza);
        });
        return msgId;
    };

    const getPrivacyTokens = async (jids) => {
        const t = unixTimestampSeconds().toString();
        const result = await query({
            tag: "iq",
            attrs: {
                to: S_WHATSAPP_NET,
                type: "set",
                xmlns: "privacy",
            },
            content: [
                {
                    tag: "tokens",
                    attrs: {},
                    content: jids.map((jid) => ({
                        tag: "token",
                        attrs: {
                            jid: jidNormalizedUser(jid),
                            t,
                            type: "trusted_contact",
                        },
                    })),
                },
            ],
        });
        return result;
    };

    const waUploadToServer = getWAUploadToServer(config, refreshMediaConn);
    
    const giftedStatus = new GrpStatus({ generateWAMessageContent: null }, waUploadToServer, relayMessage, config, sock);
    
    const waitForMsgMediaUpdate = bindWaitForEvent(ev, "messages.media-update");

    return {
        ...sock,
        getPrivacyTokens,
        assertSessions,
        relayMessage,
        sendReceipt,
        sendReceipts,
        readMessages,
        refreshMediaConn,
        waUploadToServer,
        giftedStatus,
        fetchPrivacySettings,
        sendPeerDataOperationMessage,
        createParticipantNodes,
        getUSyncDevices,
        updateMediaMessage: async (message) => {
            const content = assertMediaContent(message.message);
            const mediaKey = content.mediaKey;
            const meId = authState.creds.me.id;
            const node = await encryptMediaRetryRequest(message.key, mediaKey, meId);
            let error = undefined;
            await Promise.all([
                sendNode(node),
                waitForMsgMediaUpdate(async (update) => {
                    const result = update.find((c) => c.key.id === message.key.id);
                    if (result) {
                        if (result.error) {
                            error = result.error;
                        } else {
                            try {
                                const media = await decryptMediaRetryData(result.media, mediaKey, result.key.id);
                                if (media.result !== proto.MediaRetryNotification.ResultType.SUCCESS) {
                                    const resultStr = proto.MediaRetryNotification.ResultType[media.result];
                                    throw new Boom(`Media re-upload failed by device (${resultStr})`, {
                                        data: media,
                                        statusCode: getStatusCodeForMediaRetry(media.result) || 404,
                                    });
                                }
                                content.directPath = media.directPath;
                                content.url = getUrlFromDirectPath(content.directPath);
                                logger.debug(
                                    {
                                        directPath: media.directPath,
                                        key: result.key,
                                    },
                                    "media update successful",
                                );
                            } catch (err) {
                                error = err;
                            }
                        }
                        return true;
                    }
                }),
            ]);
            if (error) {
                throw error;
            }
            ev.emit("messages.update", [
                { key: message.key, update: { message: message.message } },
            ]);
            return message;
        },
        sendMessage: async (jid, content, options = {}) => {
            const userJid = authState.creds.me.id;
            
            if (typeof content === "object" && content.groupStatusMessage) {
                return await giftedStatus.handleGroupStory(content, jid, options.quoted);
            }
            
            if (
                typeof content === "object" &&
                "disappearingMessagesInChat" in content &&
                typeof content["disappearingMessagesInChat"] !== "undefined" &&
                isJidGroup(jid)
            ) {
                const { disappearingMessagesInChat } = content;
                const value = typeof disappearingMessagesInChat === "boolean"
                    ? disappearingMessagesInChat ? WA_DEFAULT_EPHEMERAL : 0
                    : disappearingMessagesInChat;
                await groupToggleEphemeral(jid, value);
                return;
            } 
            
            const fullMsg = await generateWAMessage(jid, content, {
                logger,
                userJid,
                getUrlInfo: (text) => getUrlInfo(text, {
                    thumbnailWidth: linkPreviewImageThumbnailWidth,
                    fetchOpts: {
                        timeout: 3000,
                        ...(axiosOptions || {}),
                    },
                    logger,
                    uploadImage: generateHighQualityLinkPreview ? waUploadToServer : undefined,
                }),
                getProfilePicUrl: sock.profilePictureUrl,
                upload: waUploadToServer,
                mediaCache: config.mediaCache,
                options: config.options,
                messageId: generateMessageIDV2(sock.user?.id),
                ...options,
            });
            
            const isDeleteMsg = "delete" in content && !!content.delete;
            const isEditMsg = "edit" in content && !!content.edit;
            const isPinMsg = "pin" in content && !!content.pin;
            const isPollMessage = "poll" in content && !!content.poll;
            
            const additionalAttributes = {};
            const additionalNodes = [];
            
            if (isDeleteMsg) {
                if (isJidGroup(content.delete?.remoteJid) && !content.delete?.fromMe) {
                    additionalAttributes.edit = "8";
                } else {
                    additionalAttributes.edit = "7";
                }
            } else if (isEditMsg) {
                additionalAttributes.edit = "1";
            } else if (isPinMsg) {
                additionalAttributes.edit = "2";
            } else if (isPollMessage) {
                additionalNodes.push({
                    tag: "meta",
                    attrs: { polltype: "creation" },
                });
            }
            
            if ("cachedGroupMetadata" in options) {
                console.warn(
                    "cachedGroupMetadata in sendMessage are deprecated, now cachedGroupMetadata is part of the socket config.",
                );
            }
            
            await relayMessage(jid, fullMsg.message, {
                messageId: fullMsg.key.id,
                useCachedGroupMetadata: options.useCachedGroupMetadata,
                additionalAttributes,
                statusJidList: options.statusJidList,
                additionalNodes,
            });
            
            if (config.emitOwnEvents) {
                process.nextTick(() => {
                    // Use messageMutex instead of processingMutex
                    messageMutex.mutex(() => upsertMessage(fullMsg, "append"));
                });
            }
            return fullMsg;
        },
    };
};