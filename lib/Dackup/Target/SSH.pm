package Dackup::Target::SSH;
use Moose;
use MooseX::StrictConstructor;
use MooseX::Types::Path::Class;
use Digest::MD5::File qw(file_md5_hex);
use File::Copy;
use Path::Class;

extends 'Dackup::Target';

has 'ssh' => (
    is       => 'ro',
    isa      => 'Net::OpenSSH',
    required => 1,
);

has 'prefix' => (
    is       => 'ro',
    isa      => 'Path::Class::Dir',
    required => 1,
    coerce   => 1,
);

__PACKAGE__->meta->make_immutable;

sub entries {
    my $self   = shift;
    my $ssh    = $self->ssh;
    my $dackup = $self->dackup;
    my $prefix = $self->prefix;
    my $cache  = $dackup->cache;

    my ( $output, $errput )
        = $ssh->capture2(
        "find $prefix -type f  | xargs stat -c '%n:%Z:%Y:%s:%i'");

    #    $ssh->error and die "ssh failed: " . $ssh->error;

    return [] unless $output;

    my @entries;
    foreach my $line ( split "\n", $output ) {

        #warn "line is [$line]";
        my ( $filename, $ctime, $mtime, $size, $inodenum ) = split ':', $line;

        #warn "[$filename / $ctime / $mtime / $size / $inodenum]";
        my $key = file($filename)->relative($prefix)->stringify;
        my $cachekey
            = 'ssh:' . $ssh->{_user} . ':' . $ssh->{_host} . ':' . $line;

        #warn "$key = $filename = [$cachekey]";

        my $md5_hex = $cache->get($cachekey);
        if ($md5_hex) {
        } else {
            my ( $md5sum_output, $md5sum_errput )
                = $ssh->capture2("md5sum $filename");
            if ($md5sum_output) {
                ($md5_hex) = split ' ', $md5sum_output;
                $cache->set( $cachekey, $md5_hex );
            } else {
                warn "missing md5sum for $filename";
                next;
            }

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

sub filename {
    my ( $self, $entry ) = @_;
    return file( $self->prefix, $entry->key );
}

sub update {
    my ( $self, $source, $entry ) = @_;
    my $ssh                   = $self->ssh;
    my $source_type           = ref($source);
    my $destination_filename  = $self->filename($entry);
    my $destination_directory = $destination_filename->parent;

    if ( $source_type eq 'Dackup::Target::Filesystem' ) {
        my $source_filename = $source->filename($entry);

        #warn "mkdir -p $destination_directory";
        $ssh->system("mkdir -p $destination_directory")
            || die "mkdir -p failed: " . $ssh->error;

        #warn "$source_filename -> $destination_filename";

        $ssh->rsync_put( "$source_filename", "$destination_filename" )
            || die "rsync failed: " . $ssh->error;

    } else {
        confess "Do not know how to update from $source_type";
    }
}

sub delete {
    my ( $self, $entry ) = @_;
    my $ssh      = $self->ssh;
    my $filename = $self->filename($entry);

    #warn "rm -f $filename";
    $ssh->system("rm -f $filename")
        || die "rm -f $filename failed: " . $ssh->error;
}

1;
