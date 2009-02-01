package Dackup::Cache;
use Moose;
use MooseX::StrictConstructor;

has 'filename' => (
    is       => 'ro',
    isa      => 'Path::Class::File',
    required => 1,
);
has 'dbh' => (
    is       => 'rw',
    isa      => 'DBI::db',
    required => 0,
);
has 'sth_select' => (
    is       => 'rw',
    isa      => 'DBI::st',
    required => 0,
);
has 'sth_insert' => (
    is       => 'rw',
    isa      => 'DBI::st',
    required => 0,
);

__PACKAGE__->meta->make_immutable;

sub BUILD {
    my $self     = shift;
    my $filename = $self->filename;

    my $exists = -f $filename;

    my $dbh = DBI->connect(
        "dbi:SQLite:dbname=$filename",
        "", "",
        {   RaiseError => 1,
            AutoCommit => 1,
        }
    );

    unless ($exists) {
        $dbh->do('PRAGMA auto_vacuum = 1');
        $dbh->do( '
CREATE TABLE md5_hex (
  id varchar NOT NULL,
  md5_hex char(32) NOT NULL,
  PRIMARY KEY (id)
)' );
    }
    $self->dbh($dbh);
    $self->sth_select(
        $dbh->prepare('SELECT md5_hex FROM md5_hex WHERE id = ?') );
    $self->sth_insert( $dbh->prepare('INSERT INTO md5_hex VALUES (?, ?)') );
}

sub get {
    my ( $self, $id ) = @_;
    my $sth = $self->sth_select;
    $sth->execute($id);
    my ($md5_hex) = $sth->fetchrow_array;
    return $md5_hex;
}

sub set {
    my ( $self, $id, $md5_hex ) = @_;
    my $sth = $self->sth_insert;
    $sth->execute( $id, $md5_hex );
}

1;
__END__

CREATE TABLE md5_hex (
  id varchar NOT NULL,
  md5_hex char(32) NOT NULL,
  PRIMARY KEY (id)
);
select datetime('now','-1 day');
