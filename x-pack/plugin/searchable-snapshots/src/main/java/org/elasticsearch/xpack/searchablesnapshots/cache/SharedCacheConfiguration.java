/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

import static org.elasticsearch.xpack.searchablesnapshots.cache.FrozenCacheService.SHARED_CACHE_SETTINGS_PREFIX;
import static org.elasticsearch.xpack.searchablesnapshots.cache.FrozenCacheService.SNAPSHOT_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.cache.FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING;

/**
 * Configuration for the shared cache. The shared cache is made up of 3 sizes of pages and are used as the separate cache regions of a
 * cached file.
 * A file's regions start and index 0 with an optional header region that can be either {@link RegionType#SMALL} or
 * {@link RegionType#TINY}, depending on the requested header cache size.
 * After (starting from either index 0 or 1 depending on whether or not there is a header region) that come as many
 * {@link RegionType#LARGE} regions as necessary to cache all bytes up to the start of the optional footer region.
 * The last region of the file is then optionally {@link RegionType#TINY} if a separate footer cache region that fits it was requested.
 *
 * The shared cache file itself contains the configured number of each page type and is split into 3 sections. First come the
 * {@link RegionType#LARGE} pages, then the {@link RegionType#SMALL} and then the {@link RegionType#TINY} pages.
 */
public final class SharedCacheConfiguration {

    public static final Setting<Float> SNAPSHOT_CACHE_SMALL_REGION_SIZE_SHARE = Setting.floatSetting(
        SHARED_CACHE_SETTINGS_PREFIX + "small_region_size_share",
        0.1f,
        0.0f,
        Setting.Property.NodeScope
    );

    public static final Setting<Float> SNAPSHOT_CACHE_TINY_REGION_SIZE_SHARE = Setting.floatSetting(
        SHARED_CACHE_SETTINGS_PREFIX + "tiny_region_size_share",
        0.01f,
        0.001f,
        Setting.Property.NodeScope
    );

    public static final long TINY_REGION_SIZE = ByteSizeValue.ofKb(1).getBytes();
    public static final long SMALL_REGION_SIZE = ByteSizeValue.ofKb(64).getBytes();

    private final long largeRegionSize;

    private final int numLargeRegions;
    private final int numSmallRegions;
    private final int numTinyRegions;

    public SharedCacheConfiguration(Settings settings) {
        long cacheSize = SNAPSHOT_CACHE_SIZE_SETTING.get(settings).getBytes();
        this.largeRegionSize = SNAPSHOT_CACHE_REGION_SIZE_SETTING.get(settings).getBytes();
        if (cacheSize > this.largeRegionSize) {
            // TODO: just forcing defaults for conflicting cache- and page sizes seems wrong
            if (largeRegionSize <= SMALL_REGION_SIZE) {
                throw new IllegalArgumentException("Large region size must be larger than small region size");
            }
            final float smallRegionShare = SNAPSHOT_CACHE_SMALL_REGION_SIZE_SHARE.get(settings);
            final float tinyRegionShare = SNAPSHOT_CACHE_TINY_REGION_SIZE_SHARE.get(settings);
            this.numLargeRegions = Math.round(Math.toIntExact(cacheSize / largeRegionSize) * (1 - smallRegionShare - tinyRegionShare));
            this.numSmallRegions = Math.max(Math.round(Math.toIntExact(cacheSize / SMALL_REGION_SIZE) * smallRegionShare), 1);
            this.numTinyRegions = Math.max(Math.round(Math.toIntExact(cacheSize / TINY_REGION_SIZE) * tinyRegionShare), 1);
            if (numLargeRegions == 0) {
                throw new IllegalArgumentException("No large regions available for the given settings");
            }
        } else {
            numLargeRegions = 0;
            numSmallRegions = 0;
            numTinyRegions = 0;
        }
    }

    public long totalSize() {
        return TINY_REGION_SIZE * numTinyRegions + SMALL_REGION_SIZE * numSmallRegions + largeRegionSize * numLargeRegions;
    }

    /**
     * Physical offset in the shared file by page number.
     */
    public long getPhysicalOffset(long sharedPageIndex) {
        assert sharedPageIndex < numLargeRegions + numSmallRegions + numTinyRegions;
        if (sharedPageIndex <= numLargeRegions) {
            // this page index is either a large region or the first small region, either way there are only large regions before it
            return sharedPageIndex * largeRegionSize;
        }
        // small regions start at this offset after all the large regions
        final long largeRegionCombinedSize = numLargeRegions * largeRegionSize;
        if (sharedPageIndex <= numLargeRegions + numSmallRegions) {
            // this page index is either a small region or the first tiny region, either way it comes after all large regions and a number
            // of small regions
            return largeRegionCombinedSize + (sharedPageIndex - numLargeRegions) * SMALL_REGION_SIZE;
        }
        // the page index is larger than the number of large- and small regions combined so its a tiny region physically located after
        // the large- and small regions combined plus a number of tiny regions
        return largeRegionCombinedSize + numSmallRegions * SMALL_REGION_SIZE + (sharedPageIndex - numSmallRegions - numLargeRegions)
            * TINY_REGION_SIZE;
    }

    /**
     * Number of large regions that can be allocated from a cache using this configuration.
     */
    public int numLargeRegions() {
        return numLargeRegions;
    }

    /**
     * Number of small regions that can be allocated from a cache using this configuration.
     */
    public int numSmallRegions() {
        return numSmallRegions;
    }

    /**
     * Number of tiny regions that can be allocated from a file using this configuration.
     */
    public int numTinyRegions() {
        return numTinyRegions;
    }

    /**
     * Computes the offset in the file at which the given region starts.
     *
     * @param region             region index
     * @param fileSize           size of the fill overall
     * @param cachedHeaderLength number of bytes that should be cached to a separate header region
     * @param footerCacheLength  number of bytes that should be cached to a separate footer region
     * @return                   offset at which the requested region starts
     */
    public long getRegionStart(int region, long fileSize, long cachedHeaderLength, long footerCacheLength) {
        assert region <= endingRegion(fileSize, cachedHeaderLength, footerCacheLength);

        if (region == 0) {
            // first region starts at the beginning of the file
            return 0L;
        }
        // since we are not in the first page the region starts at least at the size of the separately cached header page
        final boolean hasHeaderPage = cachedHeaderLength > 0;
        if (hasHeaderPage && region == 1) {
            return cachedHeaderLength;
        }

        // if the region is not the first region it can either be a large region or the separate footer cache page
        final int largeRegions = largeRegions(fileSize, cachedHeaderLength, footerCacheLength);
        // The number of large regions determines the highest region index at which a large region starts.
        // If there is a header page then its simply equal to the number of large regions, otherwise we must deduct one to go from count to
        // region index
        final int largeRegionMaxIndex = hasHeaderPage ? largeRegions : largeRegions - 1;
        if (region <= largeRegionMaxIndex) {
            return cachedHeaderLength + (region - (hasHeaderPage ? 1 : 0)) * largeRegionSize;
        }

        // the given region is the last region in the file but not a large region, it must be the footer cache region
        assert region == endingRegion(fileSize, cachedHeaderLength, footerCacheLength);
        assert footerCacheLength > 0;
        return fileSize - footerCacheLength;
    }

    // returns the index of the last region in the file
    private int endingRegion(long fileSize, long cachedHeaderSize, long footerCacheLength) {
        return getRegion(fileSize - 1, fileSize, cachedHeaderSize, footerCacheLength);
    }

    /**
     * Returns the type of region that a given region index corresponds to in a given file.
     *
     * @param region            region index
     * @param fileSize          size of the file
     * @param cacheHeaderLength size of the separate header cache region
     * @param footerCacheLength size of the separate footer cache region
     * @return region type
     */
    public RegionType regionType(int region, long fileSize, long cacheHeaderLength, long footerCacheLength) {
        assert region <= endingRegion(fileSize, cacheHeaderLength, footerCacheLength);

        if (region == 0 && cacheHeaderLength > 0) {
            // we use a header cache region and this is the first region so its either a small or tiny region
            if (cacheHeaderLength <= TINY_REGION_SIZE) {
                return RegionType.TINY;
            }
            if (cacheHeaderLength <= SMALL_REGION_SIZE) {
                return RegionType.SMALL;
            }
        }

        if (footerCacheLength > 0 && region == endingRegion(fileSize, cacheHeaderLength, footerCacheLength)) {
            // we use a footer cache region for this file and this is the last region so it must be a tiny region
            return RegionType.TINY;
        }

        // any region that isn't a separate footer or header region is large
        return RegionType.LARGE;
    }

    /**
     * Gets the index of the file region for a given position in a given file
     *
     * @param position          index in the file
     * @param fileSize          size of the file
     * @param cacheHeaderLength number of bytes to cache to a separate header page
     * @param cacheFooterLength number of bytes to cache to a separate footer page
     * @return region index
     */
    public int getRegion(long position, long fileSize, long cacheHeaderLength, long cacheFooterLength) {
        assert assertRegionParameters(fileSize, position, cacheHeaderLength, cacheFooterLength);

        // position is within the separately cached header region's length so its the first region
        if (position < cacheHeaderLength) {
            return 0;
        }

        // the position is not inside a separately cached header region so its either inside the footer region or inside a large region

        // first determine the number of large regions in this file
        final int largeRegions = largeRegions(fileSize, cacheHeaderLength, cacheFooterLength);
        final int headerRegions = cacheHeaderLength > 0 ? 1 : 0;
        if (fileSize - position <= cacheFooterLength) {
            // N large regions and one or no header regions
            return largeRegions + headerRegions;
        }

        // we are in a large region, the number of full large regions from the end of the header region plus the header region number
        // is the region index
        return Math.toIntExact((position - cacheHeaderLength) / largeRegionSize) + headerRegions;
    }

    private static boolean assertRegionParameters(long fileSize, long position, long cacheHeaderLength, long cacheFooterLength) {
        assert fileSize > 0 : "zero length file has no cache regions";
        assert position < fileSize : "Position index must be less than file size but saw [" + position + "][" + fileSize + "]";
        assert cacheFooterLength == 0 || cacheFooterLength == TINY_REGION_SIZE;
        assert cacheHeaderLength == 0 || cacheHeaderLength == TINY_REGION_SIZE || cacheHeaderLength == SMALL_REGION_SIZE;
        return true;
    }

    /**
     * Type of the shared page by shared page index.
     */
    public RegionType sharedRegionType(int sharedPageIndex) {
        if (sharedPageIndex >= numLargeRegions) {
            if (sharedPageIndex >= numLargeRegions + numSmallRegions) {
                return RegionType.TINY;
            }
            return RegionType.SMALL;
        }
        return RegionType.LARGE;
    }

    /**
     * Size of the shared page by shared page index.
     */
    public long regionSizeBySharedPageIndex(int sharedPageIndex) {
        switch (sharedRegionType(sharedPageIndex)) {
            case TINY:
                return TINY_REGION_SIZE;
            case SMALL:
                return SMALL_REGION_SIZE;
            default:
                return largeRegionSize;
        }
    }

    /**
     * Compute the size of the given region of a file.
     *
     * @param fileLength         size of file overall
     * @param region             index of the file region
     * @param cachedHeaderLength length of the header region if the header is to be cached separately or 0 if the header
     *                           is not separately cached
     * @param footerCacheLength  length of the footer region if it is separately cached or 0 of not
     *
     * @return size of the file region
     */
    public long getRegionSize(long fileLength, int region, long cachedHeaderLength, long footerCacheLength) {
        final int maxRegion = endingRegion(fileLength, cachedHeaderLength, footerCacheLength);
        if (region < maxRegion) {
            // this is not the last region so the distance between the start of this region and the next region is the size of this region
            return getRegionStart(region + 1, fileLength, cachedHeaderLength, footerCacheLength) - getRegionStart(
                region,
                fileLength,
                cachedHeaderLength,
                footerCacheLength
            );
        } else {
            // this is the last region in the file
            if (region == 0) {
                // if the file only has a single region then the size of the file is the region size
                return fileLength;
            } else {
                // the distance from the start of this region to the end of this file is the size of the region
                return fileLength - getRegionStart(region, fileLength, cachedHeaderLength, footerCacheLength);
            }
        }
    }

    /**
     * Size of the first region to use for a file that has a header to be separately cached of the given size.
     * This will either be {@link #TINY_REGION_SIZE} or {@link #SMALL_REGION_SIZE} or {@code 0} to indicate that no separate header cache
     * page should be used.
     */
    public static long effectiveHeaderCacheRange(long headerCacheRequested) {
        if (headerCacheRequested == 0L || headerCacheRequested > SMALL_REGION_SIZE) {
            // if no header cache region is requested or a small region can't fit the requested number of header cache bytes we won't use
            // a separate header cache region
            return 0L;
        }

        if (headerCacheRequested <= TINY_REGION_SIZE) {
            // it fits a tiny region so we use a tiny region to waste as little space as possible
            return TINY_REGION_SIZE;
        }

        // header region size does not fit a tiny region but we checked that it fits a small region above so we use a small region for it
        return SMALL_REGION_SIZE;
    }

    /**
     * Size of the last region to use for a file that has a footer to be separately cached of the given size.
     * This will either be {@link #TINY_REGION_SIZE} or {@code 0} to indicate that no separate header cache region should be used.
     */
    public long effectiveFooterCacheRange(long footerCacheRequested) {
        // if a non-zero length footer cache region is requested it is generally assumed to be small enough to fit a tiny region so the
        // footer will either be made up of a tiny region if possible or no footer region will be used
        return footerCacheRequested > 0 && footerCacheRequested <= TINY_REGION_SIZE ? TINY_REGION_SIZE : 0;
    }

    // calculates the number of large regions required to cache a file
    private int largeRegions(long fileLength, long cacheHeaderLength, long cacheFooterLength) {

        // large regions are only used to cache the body bytes that are not covered by the optional separate header and footer regions
        final long bodyLength = fileLength - cacheHeaderLength - cacheFooterLength;
        if (bodyLength <= 0) {
            // We do not have any large pages because header and footer cover the full file
            return 0;
        }

        // number of full large regions that are needed for the body bytes
        final int fullLargeRegions = Math.toIntExact(bodyLength / largeRegionSize);
        // large regions either align exactly and all of them are fully used or we need to add another partially used one for the remainder
        // of the body bytes
        return fullLargeRegions + (bodyLength % largeRegionSize == 0 ? 0 : 1);
    }

    public enum RegionType {
        LARGE,
        SMALL,
        TINY
    }
}
