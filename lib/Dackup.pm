package Dackup;
use Moose;
use MooseX::StrictConstructor;
use MooseX::Types::Path::Class;
use Dackup::Entry;
use Dackup::Target::Filesystem;
use Dackup::Target::S3;
use Data::Stream::Bulk::Path::Class;
use KiokuDB;
use Path::Class;
use Set::Object;

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
has 'kiokudb' => (
    is       => 'rw',
    isa      => 'KiokuDB',
    required => 0,
);

__PACKAGE__->meta->make_immutable;

sub BUILD {
    my $self = shift;
    my $filename = file( $self->directory, 'dackup.db' );
    my $kiokudb
        = KiokuDB->connect( "dbi:SQLite:dbname=$filename", create => 1 );
    $self->kiokudb($kiokudb);
}

sub backup {
    my $self        = shift;
    my $source      = $self->source;
    my $destination = $self->destination;

    #   my $scope = $self->kiokudb->new_scope;

    my $source_entries      = $source->entries($self);
    my $destination_entries = $destination->entries($self);

    my ( $entries_to_upload, $entries_to_delete )
        = $self->_calc( $source_entries, $destination_entries );

    warn 'to upload ' . scalar(@$entries_to_upload);
    warn 'to delete ' . scalar(@$entries_to_delete);
}

sub _calc {
    my ( $self, $source_entries, $destination_entries ) = @_;
    my %source_entries;
    my %destination_entries;

    $source_entries{ $_->key }      = $_ foreach @$source_entries;
    $destination_entries{ $_->key } = $_ foreach @$destination_entries;

    my @entries_to_upload;
    my @entries_to_delete;

    foreach my $key ( sort keys %source_entries ) {
        my $source_entry      = $source_entries{$key};
        my $destination_entry = $destination_entries{$key};
        if ($destination_entry) {
            if ( $source_entry->md5_hex eq $destination_entry->md5_hex ) {

                # warn "$key same";
            } else {
                warn "$key different";
                push @entries_to_upload, $source_entry;
            }
        } else {
            warn "$key missing";
            push @entries_to_upload, $source_entry;
        }
    }

    foreach my $key ( sort keys %destination_entries ) {
        my $source_entry      = $source_entries{$key};
        my $destination_entry = $destination_entries{$key};
        unless ($source_entry) {
            warn "$key to delete";
            push @entries_to_delete, $destination_entry;
        }
    }

    return \@entries_to_upload, \@entries_to_delete;
}

1;
