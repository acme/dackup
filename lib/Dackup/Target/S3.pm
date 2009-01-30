package Dackup::Target::S3;
use Moose;
use MooseX::StrictConstructor;

extends 'Dackup::Target';
has 'bucket' => (
    is       => 'ro',
    isa      => 'Net::Amazon::S3::Client::Bucket',
    required => 1,
);

__PACKAGE__->meta->make_immutable;

sub entries {
    my $self   = shift;
    my $bucket = $self->bucket;

    my @entries;
    my $object_stream = $bucket->list;
    until ( $object_stream->is_done ) {
        foreach my $object ( $object_stream->items ) {
            my $entry = Dackup::Entry->new(
                {   key     => $object->key,
                    md5_hex => $object->etag,
                    size    => $object->size,
                }
            );
            push @entries, $entry;
        }
    }
    return \@entries;
}

sub object {
    my ( $self, $entry ) = @_;
    return $self->bucket->object(
        key  => $entry->key,
        etag => $entry->md5_hex,
        size => $entry->size,
    );
}

sub put {
    my ( $self, $source, $entry ) = @_;
    my $source_type = ref($source);
    if ( $source_type eq 'Dackup::Target::Filesystem' ) {
        my $object = $self->object($entry);
        warn $entry->key;
        $object->put_filename( $source->filename($entry) );

        #        die "put one";
    } else {
        confess "Do not know how to put $source_type";
    }
}

sub delete {
    my ( $self, $entry ) = @_;

    warn $entry->key;
    my $object = $self->object($entry);
    $object->delete;

    #    die "deleted one";
}

1;