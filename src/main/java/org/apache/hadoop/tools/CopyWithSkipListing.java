package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.security.Credentials;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CopyWithSkipListing extends CopyListing {

    protected CopyListing delegate;

    public CopyWithSkipListing(Configuration configuration, Credentials credentials) {
        super(configuration, credentials);
        this.delegate = new SimpleCopyListing(getConf(), credentials);
    }


    @Override
    protected void validatePaths(DistCpOptions distCpOptions) throws IOException, InvalidInputException {
    }

    @Override
    protected void doBuildListing(Path pathToListingFile, DistCpOptions options) throws IOException {
        List<Path> globbedPaths = new ArrayList<Path>();
        if (options.getSourcePaths().isEmpty()) {
            throw new InvalidInputException("Nothing to process. Source paths::EMPTY");
        }

        for (Path path : options.getSourcePaths()) {
            FileSystem fs = path.getFileSystem(getConf());
            FileStatus[] inputs = fs.globStatus(path, new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    return !path.getName().startsWith(".");
                }
            });

            if (inputs != null && inputs.length > 0) {
                for (FileStatus onePath : inputs) {
                    globbedPaths.add(onePath.getPath());
                }
            } else {
                throw new InvalidInputException(path + " doesn't exist");
            }
        }

        DistCpOptions optionsGlobbed = new DistCpOptions(options);
        optionsGlobbed.setSourcePaths(globbedPaths);
        this.delegate.buildListing(pathToListingFile, optionsGlobbed);
    }

    @Override
    protected long getBytesToCopy() {
        return this.delegate.getBytesToCopy();
    }

    @Override
    protected long getNumberOfPaths() {
        return this.delegate.getNumberOfPaths();
    }

    public static  void main(String []args){
        
    }
}
