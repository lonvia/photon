package de.komoot.photon.nominatim.model;

import java.util.AbstractList;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class AddressRowList extends AbstractList<AddressRow> {
    private static final int MAX_RANK = 30;

    private final AddressRow[] items = new AddressRow[MAX_RANK + 1];
    private AddressRow postcode;

    @Override
    public AddressRow get(int index) {
        if (index < 0 || index > MAX_RANK) {
            throw new ArrayIndexOutOfBoundsException();
        }
        return items[index];
    }

    public AddressRow getPostcode() {
        return postcode;
    }

    @Override
    public int size() {
        return MAX_RANK + 1;
    }

    public void set(AddressRow item) {
        if (item.isPostcode()) {
            postcode = item;
        } else {
            items[item.getRankAddress()] = item;
        }
    }

    public void removeRank(int rank) {
        if (rank < 0 || rank > MAX_RANK) {
            throw new ArrayIndexOutOfBoundsException();
        }
        items[rank] = null;
    }

    public Iterator<AddressRow> reverseIterRanks() {
        return reverseIterRanks(0, MAX_RANK);
    }

    public Iterator<AddressRow> reverseIterRanks(int minRank, int maxRank) {
        while (maxRank >= minRank && items[maxRank] == null) {
            --maxRank;
        }
        final int currentRank = maxRank;

        return new Iterator<>() {
            private int rank = currentRank;

            @Override
            public boolean hasNext() {
                return rank >= minRank;
            }

            @Override
            public AddressRow next()
            {
                if (rank < minRank) {
                    throw new NoSuchElementException();
                }

                var item = items[rank--];

                while (rank >= minRank && items[rank] == null) {
                    --rank;
                }

                return item;
            }
        };
    }
}
