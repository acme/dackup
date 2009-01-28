package Dackup::Target::Filesystem;
use Moose;
use MooseX::StrictConstructor;
use MooseX::Types::Path::Class;
use Digest::MD5::File qw(file_md5_hex);

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
    my $kiokudb   = $dackup->kiokudb;

    my $scope = $kiokudb->new_scope;

    my $file_stream = Data::Stream::Bulk::Path::Class->new(
        dir        => Path::Class::Dir->new($directory),
        only_files => 1,
    );

    my @entries;
    $kiokudb->txn_do(
        sub {
            until ( $file_stream->is_done ) {
                foreach my $filename ( $file_stream->items ) {
                    my $key = $filename->relative($directory)->stringify;

                    my $stat = $filename->stat
                        || die "Unable to stat $filename";
                    my $ctime    = $stat->ctime;
                    my $mtime    = $stat->mtime;
                    my $size     = $stat->size;
                    my $inodenum = $stat->ino;
                    my $cachekey = "$filename:$ctime,$mtime,$size,$inodenum";

                    my $entry = $kiokudb->lookup($cachekey);
                    unless ($entry) {
                        $entry = Dackup::Entry->new(
                            {   filename => $filename->stringify,
                                key      => $key,
                                md5_hex  => file_md5_hex($filename),
                                size     => $size,
                            }
                        );
                        $kiokudb->store( $cachekey => $entry );
                    }
                    push @entries, $entry;
                }
            }
        }
    );
    return \@entries;
}

1;
