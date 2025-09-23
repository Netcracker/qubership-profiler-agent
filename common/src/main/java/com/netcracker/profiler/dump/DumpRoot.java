package com.netcracker.profiler.dump;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class stores information about single dump root and its files.
 *
 * @author logunov
 */
public class DumpRoot {
    private static final Logger log = LoggerFactory.getLogger(DumpRoot.class);

    private String path;
    private String name;
    private AtomicLong sizeInBytes = new AtomicLong();
    private final Queue<DumpFile> lowPriorityFiles = new LinkedList<DumpFile>();
    private final Queue<DumpFile> highPriorityFiles = new LinkedList<DumpFile>();

    private final Map<String, List<DumpFile>> filesWithDependencies = new HashMap<>();

    public DumpRoot(String path) {
        if (path == null) {
            throw new NullPointerException("Parameter path should not be null");
        }
        this.path = path;
        this.name = new File(path).getName();
    }

    /**
     * @param file file to be registered for this {@link DumpRoot}
     * @throws IllegalArgumentException if given {@code file} doesn't correspond to this {@link DumpRoot}
     */
    public void registerFile(DumpFile file) throws IllegalArgumentException {
        if (!name.equals(file.getDumpRootName())) {
            throw new IllegalArgumentException(String.format("File %s must not be registered in %s", file, this));
        }
        if(file.getDependentFile() != null) {
            addFileWithDependency(file);
        } else {
            addToQueue(file);
            String relativePath = getRelativePath(file.getPath());
            List<DumpFile> dependsOnFiles = filesWithDependencies.get(relativePath);
            if(dependsOnFiles != null) {
                for(DumpFile dependsOnFile : dependsOnFiles) {
                    dependsOnFile.setDependentFile(null);
                    addToQueue(dependsOnFile);
                }
                filesWithDependencies.remove(relativePath);
            }
        }
        sizeInBytes.getAndAdd(file.getSize());
    }

    private void addToQueue(DumpFile file) {
        Queue<DumpFile> queue = getProperQueue(file);
        queue.add(file);
    }

    private void addFileWithDependency(DumpFile file) {
        String dependentFileRelativePath = getRelativePath(file.getDependentFile().getPath());
        List<DumpFile> dumpFiles = filesWithDependencies.get(dependentFileRelativePath);
        if(dumpFiles == null) {
            dumpFiles = new LinkedList<>();
            filesWithDependencies.put(dependentFileRelativePath, dumpFiles);
        }
        dumpFiles.add(file);
    }

    private boolean removeFileWithDependency(DumpFile file) {
        String dependentFileRelativePath = getRelativePath(file.getDependentFile().getPath());
        List<DumpFile> dumpFiles = filesWithDependencies.get(dependentFileRelativePath);
        if(dumpFiles == null) {
            return false;
        }
        boolean success = dumpFiles.remove(file);
        if(dumpFiles.isEmpty()) {
            filesWithDependencies.remove(dependentFileRelativePath);
        }
        return success;
    }

    private String getRelativePath(String path) {
        return path.substring(this.path.length() + 1);
    }

    public DumpFile getFirstFile() {
        if (!lowPriorityFiles.isEmpty()) {
            return lowPriorityFiles.peek();
        }
        if (!highPriorityFiles.isEmpty()) {
            return highPriorityFiles.peek();
        }
        if (!filesWithDependencies.isEmpty()) {
            return filesWithDependencies.values().iterator().next().iterator().next();
        }
        return null;
    }

    public boolean contains(DumpFile file) {
        if(file.getDependentFile() != null) {
            for(List<DumpFile> files : filesWithDependencies.values()) {
                if(files.contains(file)) {
                    return true;
                }
            }
            return false;
        }
        Queue<DumpFile> queue = getProperQueue(file);
        return queue.contains(file);
    }

    public boolean isEmpty() {
        return lowPriorityFiles.isEmpty() && highPriorityFiles.isEmpty() && filesWithDependencies.isEmpty();
    }

    public long getSizeInBytes() {
        return sizeInBytes.get();
    }

    /**
     * @param fileDeleter instance of {@link FileDeleter}
     * @return total size of <b>retained</b> files
     */
    @SuppressWarnings("unchecked")
    public Collection<DumpFile> deleteFile(DumpFile file, FileDeleter fileDeleter) {
        boolean success;
        if(file.getDependentFile() != null) {
            success = removeFileWithDependency(file);
        } else {
            Queue<DumpFile> queue = getProperQueue(file);
            success = queue.remove(file);
        }
        if (!success) {
            log.warn("Remove wrong dump file {} from DumpRoot {}", file, this);
            return Collections.EMPTY_LIST;
        }
        deleteFile(fileDeleter, file);
        Collection<DumpFile> result;
        if (lowPriorityFiles.isEmpty()) {
            log.info("There are no 'trace'/'calls'/'xml' files in dump root {}. Delete all files", this);
            result = new ArrayList<DumpFile>(highPriorityFiles);
            for(List<DumpFile> files : filesWithDependencies.values()) {
                result.addAll(files);
            }
            result.add(file);
            erase(fileDeleter);
        } else {
            result = Collections.singletonList(file);
        }
        if (isEmpty()) {
            fileDeleter.deleteRecursively(this.path);
        }
        return result;
    }

    public boolean erase(FileDeleter fileDeleter) {
        log.info("Erasing dump root {}", this);
        lowPriorityFiles.clear();
        highPriorityFiles.clear();
        filesWithDependencies.clear();
        return fileDeleter.deleteRecursively(path);
    }

    private Queue<DumpFile> getProperQueue(DumpFile file) {
        String dirName = file.getParentDirName();
        for(String removableDir : DumpFile.REMOVABLE_DIRS) {
            if(dirName.startsWith(removableDir)) {
                return lowPriorityFiles;
            }
        }
        return highPriorityFiles;
    }

    private boolean deleteFile(FileDeleter fileDeleter, DumpFile file) {
        sizeInBytes.getAndAdd(-file.getSize());
        return fileDeleter.deleteFile(file);
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "DumpRoot{" +
                "name='" + name + '\'' +
                ", path='" + path + '\'' +
                ", size=" + getSizeInBytes() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DumpRoot dumpRoot = (DumpRoot) o;

        if (!path.equals(dumpRoot.path)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }
}
