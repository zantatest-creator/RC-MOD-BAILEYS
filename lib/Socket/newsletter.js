import { QueryIds, XWAPaths } from "../Types.js"; // .js එකතු කිරීම වැදගත්
import { generateProfilePicture } from "../Utils/messages-media.js";
import { getBinaryNodeChild } from "../WABinary.js";
import { executeWMexQuery } from "./mex.js";

const parseNewsletterCreateResponse = (response) => {
    const { id, thread_metadata: thread, viewer_metadata: viewer } = response;
    return {
        id: id,
        owner: undefined,
        name: thread.name.text,
        creation_time: parseInt(thread.creation_time, 10),
        description: thread.description.text,
        invite: thread.invite,
        subscribers: parseInt(thread.subscribers_count, 10),
        verification: thread.verification,
        picture: {
            id: thread.picture.id,
            directPath: thread.picture.direct_path
        },
        mute_state: viewer.mute
    };
};

const parseNewsletterMetadata = (result) => {
    if (typeof result !== 'object' || result === null) return null;
    if ('id' in result && typeof result.id === 'string') return result;
    if ('result' in result && typeof result.result === 'object' && result.result !== null && 'id' in result.result) {
        return result.result;
    }
    return null;
};

export const makeNewsletterSocket = (sock) => {
    const { query, generateMessageTag } = sock;
    
    const executeWMexQueryWrapper = (variables, queryId, dataPath) => {
        return executeWMexQuery(variables, queryId, dataPath, query, generateMessageTag);
    };

    const newsletterUpdate = async (jid, updates) => {
        const variables = {
            newsletter_id: jid,
            updates: { ...updates, settings: null }
        };
        return executeWMexQueryWrapper(variables, QueryIds.UPDATE_METADATA, 'xwa2_newsletter_update');
    };

    return {
        ...sock,
        newsletterCreate: async (name, description) => {
            const variables = {
                input: { name, description: description !== null ? description : null }
            };
            const rawResponse = await executeWMexQueryWrapper(variables, QueryIds.CREATE, XWAPaths.xwa2_newsletter_create);
            return parseNewsletterCreateResponse(rawResponse);
        },
        newsletterUpdate,
        newsletterSubscribers: async (jid) => {
            return executeWMexQueryWrapper({ newsletter_id: jid }, QueryIds.SUBSCRIBERS, XWAPaths.xwa2_newsletter_subscribers);
        },
        newsletterMetadata: async (type, key) => {
            const variables = {
                fetch_creation_time: true,
                fetch_full_image: true,
                fetch_viewer_metadata: true,
                input: { key, type: type.toUpperCase() }
            };
            const result = await executeWMexQueryWrapper(variables, QueryIds.METADATA, XWAPaths.xwa2_newsletter_metadata);
            return parseNewsletterMetadata(result);
        },
        newsletterFollow: async (jid) => {
            const res = await query({
                tag: 'iq',
                attrs: {
                    id: generateMessageTag(),
                    type: 'get',
                    xmlns: 'w:mex',
                    to: 's.whatsapp.net',
                },
                content: [{
                    tag: 'query',
                    attrs: { query_id: '7871414976211147' },
                    content: Buffer.from(JSON.stringify({ variables: { newsletter_id: jid } }))
                }]
            });
            if (!res?.content?.[0]) {
                throw new Error("❌ Failed to follow newsletter: unexpected response structure.");
            }
            return res;
        },
        newsletterUnfollow: (jid) => executeWMexQueryWrapper({ newsletter_id: jid }, QueryIds.UNFOLLOW, XWAPaths.xwa2_newsletter_unfollow),
        newsletterMute: (jid) => executeWMexQueryWrapper({ newsletter_id: jid }, QueryIds.MUTE, XWAPaths.xwa2_newsletter_mute_v2),
        newsletterUnmute: (jid) => executeWMexQueryWrapper({ newsletter_id: jid }, QueryIds.UNMUTE, XWAPaths.xwa2_newsletter_unmute_v2),
        newsletterUpdateName: async (jid, name) => await newsletterUpdate(jid, { name }),
        newsletterUpdateDescription: async (jid, description) => await newsletterUpdate(jid, { description }),
        newsletterUpdatePicture: async (jid, content) => {
            const { img } = await generateProfilePicture(content);
            return await newsletterUpdate(jid, { picture: img.toString('base64') });
        },
        newsletterRemovePicture: async (jid) => await newsletterUpdate(jid, { picture: '' }),
        newsletterReactMessage: async (jid, serverId, reaction) => {
            await query({
                tag: 'message',
                attrs: {
                    to: jid,
                    ...(reaction ? {} : { edit: '7' }),
                    type: 'reaction',
                    server_id: serverId,
                    id: generateMessageTag()
                },
                content: [{ tag: 'reaction', attrs: reaction ? { code: reaction } : {} }]
            });
        },
        newsletterFetchMessages: async (jid, count, since, after) => {
            const messageUpdateAttrs = { count: count.toString() };
            if (typeof since === 'number') messageUpdateAttrs.since = since.toString();
            if (after) messageUpdateAttrs.after = after.toString();
            return await query({
                tag: 'iq',
                attrs: { id: generateMessageTag(), type: 'get', xmlns: 'newsletter', to: jid },
                content: [{ tag: 'message_updates', attrs: messageUpdateAttrs }]
            });
        },
        subscribeNewsletterUpdates: async (jid) => {
            const result = await query({
                tag: 'iq',
                attrs: { id: generateMessageTag(), type: 'set', xmlns: 'newsletter', to: jid },
                content: [{ tag: 'live_updates', attrs: {}, content: [] }]
            });
            const liveUpdatesNode = getBinaryNodeChild(result, 'live_updates');
            const duration = liveUpdatesNode?.attrs?.duration;
            return duration ? { duration: duration } : null;
        },
        newsletterAdminCount: async (jid) => {
            const response = await executeWMexQueryWrapper({ newsletter_id: jid }, QueryIds.ADMIN_COUNT, XWAPaths.xwa2_newsletter_admin_count);
            return response.admin_count;
        },
        newsletterChangeOwner: async (jid, newOwnerJid) => await executeWMexQueryWrapper({ newsletter_id: jid, user_id: newOwnerJid }, QueryIds.CHANGE_OWNER, XWAPaths.xwa2_newsletter_change_owner),
        newsletterDemote: async (jid, userJid) => await executeWMexQueryWrapper({ newsletter_id: jid, user_id: userJid }, QueryIds.DEMOTE, XWAPaths.xwa2_newsletter_demote),
        newsletterDelete: async (jid) => await executeWMexQueryWrapper({ newsletter_id: jid }, QueryIds.DELETE, XWAPaths.xwa2_newsletter_delete_v2)
    };
};
