/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spi.type;

//import java.util.UUID;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import io.airlift.slice.Slice;

import java.util.UUID;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;

public final class UUIDType
    extends AbstractFixedWidthType
{
    public static final int SIZE_OF_UUID = 16; // what length?

    public static final UUIDType UUID = new UUIDType();

    private UUIDType()
    {
        super(parseTypeSignature(StandardTypes.UUID), Slice.class, SIZE_OF_UUID);
    }
//    /**
//     * Gets the name of this type which must be case insensitive globally unique.
//     * The name of a user defined type must be a legal identifier in Presto.
//     */
//    @JsonValue
//    TypeSignature getTypeSignature()
//    {
//
//    }
//
//    /**
//     * Returns the name of this type that should be displayed to end-users.
//     */
//    String getDisplayName();

        /**
         * True if the type supports equalTo and hash.
         */
        @Override
        public boolean isComparable() { return true; }

//        /**
//         * True if the type supports compareTo.
//         */
//        public boolean isOrderable() { return true; }
//
//    /**
//     * Gets the Java class type used to represent this value on the stack during
//     * expression execution. This value is used to determine which method should
//     * be called on Cursor, RecordSet or RandomAccessBlock to fetch a value of
//     * this type.
//     *
//     * Currently, this must be boolean, long, double, or Slice.
//     */
//    @Override
//    public Class<?> getJavaType() { return Slice.class; }
//
//    /**
//     * For parameterized types returns the list of parameters.
//     */
//    List<Type> getTypeParameters();
//
//    /**
//     * Creates the preferred block builder for this type. This is the builder used to
//     * store values after an expression projection within the query.
//     */
//    BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry);
//
//    /**
//     * Creates the preferred block builder for this type. This is the builder used to
//     * store values after an expression projection within the query.
//     */
//    BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries);
//
//    /**
//     * Gets an object representation of the type value in the {@code block}
//     * {@code position}. This is the value returned to the user via the
//     * REST endpoint and therefore must be JSON serializable.
//     */
//    @Override
        public Object getObjectValue(ConnectorSession session, Block block, int position)
        {
            if (block.isNull(position)) {
                return null;
            }

            return block.getSlice(position, 0, SIZE_OF_UUID);
        }
//
//    /**
//     * Gets the value at the {@code block} {@code position} as a boolean.
//     */
//    boolean getBoolean(Block block, int position);
//
//    /**
//     * Gets the value at the {@code block} {@code position} as a long.
//     */
//    long getLong(Block block, int position);
//
//    /**
//     * Gets the value at the {@code block} {@code position} as a double.
//     */
//    double getDouble(Block block, int position);
//
//    /**
//     * Writes the boolean value into the {@code BlockBuilder}.
//     */
//    void writeBoolean(BlockBuilder blockBuilder, boolean value);
//
//    /**
//     * Writes the long value into the {@code BlockBuilder}.
//     */
//    void writeLong(BlockBuilder blockBuilder, long value);
//
//    /**
//     * Writes the double value into the {@code BlockBuilder}.
//     */
//    void writeDouble(BlockBuilder blockBuilder, double value);
//
    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        writeSlice(blockBuilder, value, 0, SIZE_OF_UUID);
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        blockBuilder.writeBytes(value, offset, length).closeEntry();
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeBytes(block.getSlice(position, 0, SIZE_OF_UUID), 0, SIZE_OF_UUID).closeEntry();
        }
    }
    /**
     * Are the values in the specified blocks at the specified positions equal?
     */
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        Slice leftValue = leftBlock.getSlice(leftPosition, 0, SIZE_OF_UUID);
//        Slice rightValue = rightBlock.getSlice(rightPosition, 0, SIZE_OF_UUID);
//        return leftValue == rightValue;
//        int leftLength = leftBlock.getLength(leftPosition);
//        int rightLength = rightBlock.getLength(rightPosition);
//        if (leftLength != rightLength) {
//            return false;
//        }
        return leftBlock.equals(leftPosition, 0, rightBlock, rightPosition, 0, SIZE_OF_UUID);
    }

    /**
     * Calculates the hash code of the value at the specified position in the
     * specified block.
     */
    @Override
    public int hash(Block block, int position)
    {
        return block.getSlice(position, 0, SIZE_OF_UUID).hashCode();
    }

//    /**
//     * Compare the values in the specified block at the specified positions equal.
//     */
//    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
//    {
//        java.util.UUID left = java.util.UUID.fromString(leftBlock.getSlice(leftPosition, 0, SIZE_OF_UUID).toStringUtf8());
//        java.util.UUID right = java.util.UUID.fromString(leftBlock.getSlice(rightPosition, 0, SIZE_OF_UUID).toStringUtf8());;
//
//        return left.compareTo(right);
//    }
}
