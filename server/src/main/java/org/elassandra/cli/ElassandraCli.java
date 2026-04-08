package org.elassandra.cli;

import org.apache.lucene.util.IOUtils;
import org.opensearch.cli.Command;
import org.opensearch.cli.LoggingAwareMultiCommand;
import org.opensearch.cli.Terminal;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class ElassandraCli extends LoggingAwareMultiCommand {

    private final Collection<Command> commands;

    public ElassandraCli() {
        super("A tool for various Elassandra actions");
        subcommands.put("decodeMetadata", new DecodeMetaDataCommand());
        subcommands.put("decodeIndexMetadata", new DecodeIndexMetaDataCommand());
        commands = Collections.unmodifiableCollection(subcommands.values());
    }

    public static void main(String[] args) throws Exception {
        exit(new ElassandraCli().main(args, Terminal.DEFAULT));
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(commands);
    }

}
