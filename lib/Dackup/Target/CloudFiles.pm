package Dackup::Target::CloudFiles;
use Moose;
use MooseX::StrictConstructor;
use File::Temp qw/tmpnam/;

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
    my $prefix    = $self->prefix;

    my @entries;
    foreach my $object ( $container->objects( prefix => $prefix )->all ) {
        my $key = $object->name;
        $key =~ s/^$prefix//;
        my $cachekey
            = 'cloudfiles:' . $container->name . ':' . $prefix . $key;
        my $size_md5_hex = $cache->get($cachekey);
        my ( $size, $md5_hex );
        if ($size_md5_hex) {
            ( $size, $md5_hex ) = split ' ', $size_md5_hex;
        } else {
            $size    = $object->size || 0;
            $md5_hex = $object->md5  || '';
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
    my $cache       = $self->dackup->cache;
    my $prefix      = $self->prefix;
    my $source_type = ref($source);
    if ( $source_type eq 'Dackup::Target::Filesystem' ) {
        $container->put_filename( $prefix . $entry->key,
            $source->filename($entry) );
    } elsif ( $source_type eq 'Dackup::Target::S3' ) {
        my $filename      = tmpnam();
        my $source_object = $source->object($entry);
        $source_object->get_filename($filename);
        $container->put_filename( $prefix . $entry->key, $filename );
        unlink($filename) || die "Error deleting $filename: $!";
    } else {
        confess "Do not know how to update from $source_type";
    }
    my $cachekey
        = 'cloudfiles:' . $container->name . ':' . $prefix . $entry->key;
    $cache->delete($cachekey);
    $cache->set( $cachekey, $entry->size . ' ' . $entry->md5_hex );
}

sub delete {
    my ( $self, $entry ) = @_;
    my $container = $self->container;
    my $prefix    = $self->prefix;
    my $object    = $container->object( $prefix . $entry->key );
    $object->delete;
    my $cachekey
        = 'cloudfiles:' . $container->name . ':' . $prefix . $entry->key;
    $self->dackup->cache->delete($cachekey);
}

1;
