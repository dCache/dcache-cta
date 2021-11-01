/**
 * Copyright (C) 2011-2018 dCache.org <support@dcache.org>
 *
 * This file is part of xrootd4j.
 *
 * xrootd4j is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * xrootd4j is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with xrootd4j.  If not, see http://www.gnu.org/licenses/.
 */
package org.dcache.nearline.cta.xrootd;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.Charset;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A simple builder-pattern class to create a ByteBuf with specific contents.
 * This is hopefully an easier to understand and document than raw bytes.
 */
class ByteBufBuilder
{
    private final ByteBuf buffer = Unpooled.buffer();

    public ByteBufBuilder withString(String value, Charset charset) {
        buffer.writeInt(value.getBytes(charset).length);
        buffer.writeCharSequence(value, charset);
        return this;
    }

    /** String is written and any left-over space is padded with zeros. */
    public ByteBufBuilder withFixedSizeString(int length, String value, Charset charset) {
        byte[] data = value.getBytes(charset);
        checkArgument(data.length <= length);
        buffer.writeBytes(data);
        buffer.writeZero(length - data.length);
        return this;
    }

    public ByteBufBuilder withInt(int value) {
        buffer.writeInt(value);
        return this;
    }

    public ByteBufBuilder withShort(int value) {
        buffer.writeShort(value);
        return this;
    }

    public ByteBufBuilder withByte(int value) {
        buffer.writeByte(value);
        return this;
    }

    public ByteBufBuilder withZeros(int count) {
        buffer.writeZero(count);
        return this;
    }

    public ByteBuf build() {
        return buffer;
    }
}
