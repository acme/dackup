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
                {   filename => undef,
                    key      => $object->key,
                    md5_hex  => $object->etag,
                    size     => $object->size,
                }
            );
            push @entries, $entry;
        }
    }
    return \@entries;
}

1;
