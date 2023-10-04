package cal.prim.fs;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.file.Path;
import java.time.Instant;

/**
 * Information about a regular (i.e. non-link) file.
 *
 * <p>The metadata getters {@link #path()}, {@link #modTime()}, {@link #sizeInBytes()},
 * and {@link #iNode()} are pure and return information about an atomic snapshot of the file
 * at some point in the past.
 *
 * @param iNode
 *     The "inode" number of the file.
 *     On POSIX filesystems, this can be used to detect
 *     hard links: two files that share an inode number
 *     also share their contents.
 */
public record RegularFile(Path path, Instant modTime, long sizeInBytes, @Nullable Object iNode) {
}
