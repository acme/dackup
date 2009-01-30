package Dackup::Target::Filesystem;
use Moose;
use MooseX::StrictConstructor;
use MooseX::Types::Path::Class;
use Digest::MD5::File qw(file_md5_hex);
use File::Copy;
use Path::Class;

extends 'Dackup::Target';

has 'prefix' => (
    is       => 'ro',
    isa      => 'Path::Class::Dir',
    required => 1,
    coerce   => 1,
);

__PACKAGE__->meta->make_immutable;

sub entries {
    my $self   = shift;
    my $dackup = shift;
    my $prefix = $self->prefix;
    my $cache  = $dackup->cache;

    return [] unless -d $prefix;

    my $file_stream = Data::Stream::Bulk::Path::Class->new(
        dir        => Path::Class::Dir->new($prefix),
        only_files => 1,
    );

    my @entries;
    until ( $file_stream->is_done ) {
        foreach my $filename ( $file_stream->items ) {
            my $key = $filename->relative($prefix)->stringify;

            my $stat = $filename->stat
                || confess "Unable to stat $filename";
            my $ctime    = $stat->ctime;
            my $mtime    = $stat->mtime;
            my $size     = $stat->size;
            my $inodenum = $stat->ino;
            my $cachekey = "$filename:$ctime,$mtime,$size,$inodenum";

            my $md5_hex = $cache->get($cachekey);
            if ($md5_hex) {
            } else {
                $md5_hex = file_md5_hex($filename);
                $cache->set( $cachekey, $md5_hex );
            }

            my $entry = Dackup::Entry->new(
                {   key     => $key,
                    md5_hex => $md5_hex,
                    size    => $size,
                }
            );
            push @entries, $entry;
        }
    }
    return \@entries;
}

sub filename {
    my ( $self, $entry ) = @_;
    return file( $self->prefix, $entry->key );
}

sub put {
    my ( $self, $source, $entry ) = @_;
    my $source_type          = ref($source);
    my $destination_filename = $self->filename($entry);
    $destination_filename->parent->mkpath;

    if ( $source_type eq 'Dackup::Target::Filesystem' ) {
        my $source_filename = $source->filename($entry);
        copy( $source_filename->stringify, $destination_filename->stringify )
            || confess(
            "Error copying $source_filename to $destination_filename: $!");
    } elsif ( $source_type eq 'Dackup::Target::S3' ) {
        my $source_object = $source->object($entry);
        $source_object->get_filename( $destination_filename->stringify );
    } else {
        confess "Do not know how to put $source_type";
    }
}

sub delete {
    my ( $self, $entry ) = @_;
    my $filename = $self->filename($entry);
    unlink($filename) || confess("Error deleting $filename: $!");
}

1;
