//////////////////////////////////////////////////////////
// This file was auto-generated by java_bindings.zig
// Do not manually modify.
//////////////////////////////////////////////////////////

package com.tigerbeetle;

import java.nio.ByteBuffer;
import java.math.BigInteger;

public final class AccountBalanceBatch extends Batch {

    interface Struct {
        int SIZE = 128;

        int DebitsPending = 0;
        int DebitsPosted = 16;
        int CreditsPending = 32;
        int CreditsPosted = 48;
        int Timestamp = 64;
        int Reserved = 72;
    }

    static final AccountBalanceBatch EMPTY = new AccountBalanceBatch(0);

    /**
     * Creates an empty batch with the desired maximum capacity.
     * <p>
     * Once created, an instance cannot be resized, however it may contain any number of elements
     * between zero and its {@link #getCapacity capacity}.
     *
     * @param capacity the maximum capacity.
     * @throws IllegalArgumentException if capacity is negative.
     */
    public AccountBalanceBatch(final int capacity) {
        super(capacity, Struct.SIZE);
    }

    AccountBalanceBatch(final ByteBuffer buffer) {
        super(buffer, Struct.SIZE);
    }

    /**
     * @return a {@link java.math.BigInteger} representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#debits_pending">debits_pending</a>
     */
    public BigInteger getDebitsPending() {
        final var index = at(Struct.DebitsPending);
        return UInt128.asBigInteger(
            getUInt128(index, UInt128.LeastSignificant), 
            getUInt128(index, UInt128.MostSignificant));
    }

    /**
     * @param part a {@link UInt128} enum indicating which part of the 128-bit value is to be retrieved.
     * @return a {@code long} representing the first 8 bytes of the 128-bit value if
     *         {@link UInt128#LeastSignificant} is informed, or the last 8 bytes if
     *         {@link UInt128#MostSignificant}.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#debits_pending">debits_pending</a>
     */
    public long getDebitsPending(final UInt128 part) {
        return getUInt128(at(Struct.DebitsPending), part);
    }

    /**
     * @param debitsPending a {@link java.math.BigInteger} representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#debits_pending">debits_pending</a>
     */
    void setDebitsPending(final BigInteger debitsPending) {
        putUInt128(at(Struct.DebitsPending), UInt128.asBytes(debitsPending));
    }

    /**
     * @param leastSignificant a {@code long} representing the first 8 bytes of the 128-bit value.
     * @param mostSignificant a {@code long} representing the last 8 bytes of the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#debits_pending">debits_pending</a>
     */
    void setDebitsPending(final long leastSignificant, final long mostSignificant) {
        putUInt128(at(Struct.DebitsPending), leastSignificant, mostSignificant);
    }

    /**
     * @param leastSignificant a {@code long} representing the first 8 bytes of the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#debits_pending">debits_pending</a>
     */
    void setDebitsPending(final long leastSignificant) {
        putUInt128(at(Struct.DebitsPending), leastSignificant, 0);
    }

    /**
     * @return a {@link java.math.BigInteger} representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#debits_posted">debits_posted</a>
     */
    public BigInteger getDebitsPosted() {
        final var index = at(Struct.DebitsPosted);
        return UInt128.asBigInteger(
            getUInt128(index, UInt128.LeastSignificant), 
            getUInt128(index, UInt128.MostSignificant));
    }

    /**
     * @param part a {@link UInt128} enum indicating which part of the 128-bit value is to be retrieved.
     * @return a {@code long} representing the first 8 bytes of the 128-bit value if
     *         {@link UInt128#LeastSignificant} is informed, or the last 8 bytes if
     *         {@link UInt128#MostSignificant}.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#debits_posted">debits_posted</a>
     */
    public long getDebitsPosted(final UInt128 part) {
        return getUInt128(at(Struct.DebitsPosted), part);
    }

    /**
     * @param debitsPosted a {@link java.math.BigInteger} representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#debits_posted">debits_posted</a>
     */
    void setDebitsPosted(final BigInteger debitsPosted) {
        putUInt128(at(Struct.DebitsPosted), UInt128.asBytes(debitsPosted));
    }

    /**
     * @param leastSignificant a {@code long} representing the first 8 bytes of the 128-bit value.
     * @param mostSignificant a {@code long} representing the last 8 bytes of the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#debits_posted">debits_posted</a>
     */
    void setDebitsPosted(final long leastSignificant, final long mostSignificant) {
        putUInt128(at(Struct.DebitsPosted), leastSignificant, mostSignificant);
    }

    /**
     * @param leastSignificant a {@code long} representing the first 8 bytes of the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#debits_posted">debits_posted</a>
     */
    void setDebitsPosted(final long leastSignificant) {
        putUInt128(at(Struct.DebitsPosted), leastSignificant, 0);
    }

    /**
     * @return a {@link java.math.BigInteger} representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#credits_pending">credits_pending</a>
     */
    public BigInteger getCreditsPending() {
        final var index = at(Struct.CreditsPending);
        return UInt128.asBigInteger(
            getUInt128(index, UInt128.LeastSignificant), 
            getUInt128(index, UInt128.MostSignificant));
    }

    /**
     * @param part a {@link UInt128} enum indicating which part of the 128-bit value is to be retrieved.
     * @return a {@code long} representing the first 8 bytes of the 128-bit value if
     *         {@link UInt128#LeastSignificant} is informed, or the last 8 bytes if
     *         {@link UInt128#MostSignificant}.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#credits_pending">credits_pending</a>
     */
    public long getCreditsPending(final UInt128 part) {
        return getUInt128(at(Struct.CreditsPending), part);
    }

    /**
     * @param creditsPending a {@link java.math.BigInteger} representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#credits_pending">credits_pending</a>
     */
    void setCreditsPending(final BigInteger creditsPending) {
        putUInt128(at(Struct.CreditsPending), UInt128.asBytes(creditsPending));
    }

    /**
     * @param leastSignificant a {@code long} representing the first 8 bytes of the 128-bit value.
     * @param mostSignificant a {@code long} representing the last 8 bytes of the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#credits_pending">credits_pending</a>
     */
    void setCreditsPending(final long leastSignificant, final long mostSignificant) {
        putUInt128(at(Struct.CreditsPending), leastSignificant, mostSignificant);
    }

    /**
     * @param leastSignificant a {@code long} representing the first 8 bytes of the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#credits_pending">credits_pending</a>
     */
    void setCreditsPending(final long leastSignificant) {
        putUInt128(at(Struct.CreditsPending), leastSignificant, 0);
    }

    /**
     * @return a {@link java.math.BigInteger} representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#credits_posted">credits_posted</a>
     */
    public BigInteger getCreditsPosted() {
        final var index = at(Struct.CreditsPosted);
        return UInt128.asBigInteger(
            getUInt128(index, UInt128.LeastSignificant), 
            getUInt128(index, UInt128.MostSignificant));
    }

    /**
     * @param part a {@link UInt128} enum indicating which part of the 128-bit value is to be retrieved.
     * @return a {@code long} representing the first 8 bytes of the 128-bit value if
     *         {@link UInt128#LeastSignificant} is informed, or the last 8 bytes if
     *         {@link UInt128#MostSignificant}.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#credits_posted">credits_posted</a>
     */
    public long getCreditsPosted(final UInt128 part) {
        return getUInt128(at(Struct.CreditsPosted), part);
    }

    /**
     * @param creditsPosted a {@link java.math.BigInteger} representing the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#credits_posted">credits_posted</a>
     */
    void setCreditsPosted(final BigInteger creditsPosted) {
        putUInt128(at(Struct.CreditsPosted), UInt128.asBytes(creditsPosted));
    }

    /**
     * @param leastSignificant a {@code long} representing the first 8 bytes of the 128-bit value.
     * @param mostSignificant a {@code long} representing the last 8 bytes of the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#credits_posted">credits_posted</a>
     */
    void setCreditsPosted(final long leastSignificant, final long mostSignificant) {
        putUInt128(at(Struct.CreditsPosted), leastSignificant, mostSignificant);
    }

    /**
     * @param leastSignificant a {@code long} representing the first 8 bytes of the 128-bit value.
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#credits_posted">credits_posted</a>
     */
    void setCreditsPosted(final long leastSignificant) {
        putUInt128(at(Struct.CreditsPosted), leastSignificant, 0);
    }

    /**
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#timestamp">timestamp</a>
     */
    public long getTimestamp() {
        final var value = getUInt64(at(Struct.Timestamp));
        return value;
    }

    /**
     * @param timestamp
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#timestamp">timestamp</a>
     */
    void setTimestamp(final long timestamp) {
        putUInt64(at(Struct.Timestamp), timestamp);
    }

    /**
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#reserved">reserved</a>
     */
    byte[] getReserved() {
        return getArray(at(Struct.Reserved), 56);
    }

    /**
     * @param reserved
     * @throws IllegalStateException if not at a {@link #isValidPosition valid position}.
     * @throws IllegalStateException if a {@link #isReadOnly() read-only} batch.
     * @see <a href="https://docs.tigerbeetle.com/reference/account_filter/#reserved">reserved</a>
     */
    void setReserved(byte[] reserved) {
        if (reserved == null)
            reserved = new byte[56];
        if (reserved.length != 56)
            throw new IllegalArgumentException("Reserved must be 56 bytes long");
        putArray(at(Struct.Reserved), reserved);
    }

}

