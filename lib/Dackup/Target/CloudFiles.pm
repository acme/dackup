package Dackup::Target::CloudFiles;
use Moose;
use MooseX::StrictConstructor;

extends 'Dackup::Target';

has 'container' => (
    is       => 'ro',
    isa      => 'Net::Mosso::CloudFiles::Container',
    required => 1,
);

has 'prefix' => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
    default  => '',
);

__PACKAGE__->meta->make_immutable;

sub entries {
    my $self      = shift;
    my $dackup    = $self->dackup;
    my $cache     = $dackup->cache;
    my $container = $self->container;

    my @entries;
    foreach my $object ( $container->objects->all ) {
        my $key          = $object->name;
        my $cachekey     = 'cloudfiles:' . $container->name . ':' . $key;
        my $size_md5_hex = $cache->get($cachekey);
        my ( $size, $md5_hex );
        if ($size_md5_hex) {
            ( $size, $md5_hex ) = split ' ', $size_md5_hex;
        } else {
            $size    = $object->size;
            $md5_hex = $object->md5;
            $cache->set( $cachekey, "$size $md5_hex" );
        }
        my $entry = Dackup::Entry->new(
            {   key     => $key,
                md5_hex => $md5_hex,
                size    => $size,
            }
        );
        push @entries, $entry;
    }
    return \@entries;
}

sub update {
    my ( $self, $source, $entry ) = @_;
    my $container   = $self->container;
    my $source_type = ref($source);
    if ( $source_type eq 'Dackup::Target::Filesystem' ) {
        $container->put_filename( $entry->key, $source->filename($entry) );
    } else {
        confess "Do not know how to update from $source_type";
    }
    my $cachekey = 'cloudfiles:' . $container->name . ':' . $entry->key;
    $self->dackup->cache->set( $cachekey,
        $entry->size . ' ' . $entry->md5_hex );
}

sub delete {
    my ( $self, $entry ) = @_;
    my $container = $self->container;
    $container->delete( $entry->key );
    my $cachekey = 'cloudfiles:' . $container->name . ':' . $entry->key;
    $self->dackup->cache->set( $cachekey,
        $entry->size . ' ' . $entry->md5_hex );
}

1;
