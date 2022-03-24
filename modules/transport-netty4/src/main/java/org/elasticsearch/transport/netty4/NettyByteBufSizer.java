/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty4;

import io.netty5.buffer.ByteBuf;
import io.netty5.channel.ChannelHandler;
import io.netty5.channel.ChannelHandlerContext;
import io.netty5.handler.codec.MessageToMessageDecoder;

@ChannelHandler.Sharable
public class NettyByteBufSizer extends MessageToMessageDecoder<ByteBuf> {

    public static final NettyByteBufSizer INSTANCE = new NettyByteBufSizer();

    private NettyByteBufSizer() {
        // sharable singleton
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buf) {
        int readableBytes = buf.readableBytes();
        if (buf.capacity() >= 1024) {
            ByteBuf resized = buf.discardReadBytes().capacity(readableBytes);
            assert resized.readableBytes() == readableBytes;
            ctx.fireChannelRead(resized.retain());
        } else {
            ctx.fireChannelRead(buf.retain());
        }
    }
}
