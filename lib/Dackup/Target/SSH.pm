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

has 'directories' => (
    is       => 'rw',
    isa      => 'HashRef',
    required => 0,
    default  => sub { {} },
);

__PACKAGE__->meta->make_immutable;

sub entries {
    my $self        = shift;
    my $ssh         = $self->ssh;
    my $dackup      = $self->dackup;
    my $prefix      = $self->prefix;
    my $cache       = $dackup->cache;
    my $directories = $self->directories;

    my ( $type, $type_err ) = $ssh->capture2("stat -c '%F' $prefix");
    chomp $type;
    return [] if $type ne 'directory';

    my ( $output, $errput )
        = $ssh->capture2("find $prefix | xargs stat -c '%F:%n:%Z:%Y:%s:%i'");
    $ssh->error and die "ssh failed: " . $ssh->error;

    return [] unless $output;

    my @entries;
    my @not_in_cache;
    foreach my $line ( split "\n", $output ) {
        my ( $type, $filename, $ctime, $mtime, $size, $inodenum ) = split ':',
            $line;
        confess "Error with stat: $line"
            unless $type
                && defined($filename)
                && $ctime
                && $mtime
                && defined($size)
                && defined($inodenum);

        if ( $type eq 'directory' ) {
            $directories->{$filename} = 1;
            next;
        }

        my $key = file($filename)->relative($prefix)->stringify;
        my $cachekey
            = 'ssh:' . $ssh->{_user} . ':' . $ssh->{_host} . ':' . $line;

        my $md5_hex = $cache->get($cachekey);
        if ($md5_hex) {
            push @entries,
                Dackup::Entry->new(
                {   key     => $key,
                    md5_hex => $md5_hex,
                    size    => $size,
                }
                );
        } else {
            push @not_in_cache,
                {
                key      => $key,
                cachekey => $cachekey,
                filename => $filename,
                size     => $size,
                };
        }
    }

    my $tempfile = $ssh->capture('tempfile');
    chomp $tempfile;
    $ssh->error and die "ssh failed: " . $ssh->error;
    die "missing $tempfile" unless $tempfile;
    my ( $rin, $in_pid ) = $ssh->pipe_in("cat > $tempfile")
        or die "pipe_in method failed: " . $ssh->error;

    my %filename_to_d;
    foreach my $d (@not_in_cache) {
        my $filename = $d->{filename};
        $rin->print("$filename\n") || die $ssh->error;
        $filename_to_d{$filename} = $d;
    }
    $rin->close || die $ssh->error;
    waitpid( $in_pid, 0 );

    my $lines = $ssh->capture("xargs --arg-file $tempfile md5sum")
        or die "capture method failed: " . $ssh->error;
    foreach my $line ( split "\n", $lines ) {

        # chomp $line;
        #warn "[$line]";
        my ( $md5_hex, $filename ) = split / +/, $line;

        #warn "[$md5_hex, $filename]";
        confess "Error with $line"
            unless defined $md5_hex && defined $filename;
        my $d = $filename_to_d{$filename};
        confess "Missing d for $filename" unless $d;
        push @entries,
            Dackup::Entry->new(
            {   key     => $d->{key},
                md5_hex => $md5_hex,
                size    => $d->{size},
            }
            );
        $cache->set( $d->{cachekey}, $md5_hex );
    }
    $ssh->system("rm $tempfile")
        or die "remote command failed: " . $ssh->error;

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
    my $directories           = $self->directories;

    if ( $source_type eq 'Dackup::Target::Filesystem' ) {
        my $source_filename = $source->filename($entry);

        unless ( $directories->{$destination_directory} ) {

            #warn "mkdir -p $destination_directory";
            $ssh->system("mkdir -p $destination_directory")
                || die "mkdir -p failed: " . $ssh->error;
            $directories->{$destination_directory} = 1;
        }

        #warn "$source_filename -> $destination_filename";

        $ssh->scp_put( "$source_filename", "$destination_filename" )
            || die "scp failed: " . $ssh->error;
    } else {
        confess "Do not know how to update from $source_type";
    }
}

sub delete {
    my ( $self, $entry ) = @_;
    my $ssh      = $self->ssh;
    my $filename = $self->filename($entry);

    $ssh->system("rm -f $filename")
        || die "rm -f $filename failed: " . $ssh->error;
}

1;

__END__

=head1 NAME

Dackup::Target::SSH - Flexible file backup remote hosts via SSH

=head1 SYNOPSIS

  use Dackup;
  use Net::OpenSSH;

  my $ssh = Net::OpenSSH->new('acme:password@backuphost');
  $ssh->error
      and die "Couldn't establish SSH connection: " . $ssh->error;

  my $source = Dackup::Target::Filesystem->new(
      prefix => '/home/acme/important/' );

  my $destination = Dackup::Target::SSH->new(
      ssh    => $ssh,
      prefix => '/home/acme/important_backup/'
  );

  my $dackup = Dackup->new(
      directory   => '/home/acme/dackup',
      source      => $source,
      destination => $destination,
      delete      => 0,
  );
  $dackup->backup;

=head1 DESCRIPTION

This is a Dackup target for a remote host via SSH.

=head1 AUTHOR

Leon Brocard <acme@astray.com>

=head1 COPYRIGHT

Copyright (C) 2009, Leon Brocard.

=head1 LICENSE

This module is free software; you can redistribute it or 
modify it under the same terms as Perl itself.
