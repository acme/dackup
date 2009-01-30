package Dackup::Target::Filesystem;
use Moose;
use MooseX::StrictConstructor;
use MooseX::Types::Path::Class;
use Digest::MD5::File qw(file_md5_hex);
use Path::Class;

extends 'Dackup::Target';
has 'directory' => (
    is       => 'ro',
    isa      => 'Path::Class::Dir',
    required => 1,
    coerce   => 1,
);

__PACKAGE__->meta->make_immutable;

sub entries {
    my $self      = shift;
    my $dackup    = shift;
    my $directory = $self->directory;
    my $cache     = $dackup->cache;

    my $file_stream = Data::Stream::Bulk::Path::Class->new(
        dir        => Path::Class::Dir->new($directory),
        only_files => 1,
    );

    my @entries;
    until ( $file_stream->is_done ) {
        foreach my $filename ( $file_stream->items ) {
            my $key = $filename->relative($directory)->stringify;

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
    return file( $self->directory, $entry->key );
}

1;
