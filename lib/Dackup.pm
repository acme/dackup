package Dackup;
use Moose;
use MooseX::StrictConstructor;
use MooseX::Types::Path::Class;
use Dackup::Cache;
use Dackup::Entry;
use Dackup::Target::CloudFiles;
use Dackup::Target::Filesystem;
use Dackup::Target::S3;
use DBI;
use Data::Stream::Bulk::Path::Class;
use Path::Class;
use Term::ProgressBar::Simple;

has 'directory' => (
    is       => 'ro',
    isa      => 'Path::Class::Dir',
    required => 1,
    coerce   => 1,
);
has 'source' => (
    is       => 'ro',
    isa      => 'Dackup::Target',
    required => 1,
);
has 'destination' => (
    is       => 'ro',
    isa      => 'Dackup::Target',
    required => 1,
);
has 'cache' => (
    is       => 'rw',
    isa      => 'Dackup::Cache',
    required => 0,
);

__PACKAGE__->meta->make_immutable;

sub BUILD {
    my $self     = shift;
    my $filename = file( $self->directory, 'dackup.db' );
    my $cache    = Dackup::Cache->new( filename => $filename );
    $self->cache($cache);
    $self->source->dackup($self);
    $self->destination->dackup($self);
}

sub backup {
    my $self        = shift;
    my $source      = $self->source;
    my $destination = $self->destination;

    my $source_entries      = $source->entries;
    my $destination_entries = $destination->entries;

    my ( $entries_to_update, $entries_to_delete )
        = $self->_calc( $source_entries, $destination_entries );

    warn 'to update ' . scalar(@$entries_to_update);
    warn 'to delete ' . scalar(@$entries_to_delete);

    my $progress = Term::ProgressBar::Simple->new(
        scalar(@$entries_to_update) + scalar(@$entries_to_delete) );
    foreach my $entry (@$entries_to_update) {
        $destination->update( $source, $entry );
        $progress++;
    }
    foreach my $entry (@$entries_to_delete) {
        $destination->delete($entry);
        $progress++;
    }
}

sub _calc {
    my ( $self, $source_entries, $destination_entries ) = @_;
    my %source_entries;
    my %destination_entries;

    $source_entries{ $_->key }      = $_ foreach @$source_entries;
    $destination_entries{ $_->key } = $_ foreach @$destination_entries;

    my @entries_to_update;
    my @entries_to_delete;

    foreach my $key ( sort keys %source_entries ) {
        my $source_entry      = $source_entries{$key};
        my $destination_entry = $destination_entries{$key};
        if ($destination_entry) {
            if ( $source_entry->md5_hex eq $destination_entry->md5_hex ) {

                # warn "$key same";
            } else {

                # warn "$key different";
                push @entries_to_update, $source_entry;
            }
        } else {

            # warn "$key missing";
            push @entries_to_update, $source_entry;
        }
    }

    foreach my $key ( sort keys %destination_entries ) {
        my $source_entry      = $source_entries{$key};
        my $destination_entry = $destination_entries{$key};
        unless ($source_entry) {

            # warn "$key to delete";
            push @entries_to_delete, $destination_entry;
        }
    }

    return \@entries_to_update, \@entries_to_delete;
}

1;
