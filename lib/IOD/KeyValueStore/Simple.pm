package IOD::KeyValueStore::Simple;

# AUTHORITY
# DATE
# DIST
# VERSION

use 5.010001;
use strict;
use warnings;
use Log::ger;

sub new {
    my ($class, %args) = @_;

    my $path = delete $args{path};
    $path //= do {
        $ENV{HOME} or die "HOME not defined, can't set default for path";
        "$ENV{HOME}/kvstore.iod";
    };

    unless (-f $path) {
        log_trace "Creating IOD key-value store file '$path' ...";
        open my $fh, ">>", $path or die "Can't open IOD key-value store file '$path': $!";
    }

    my $section = delete($args{section}) // 'keyvaluestore';

    require Config::IOD;
    my $iod = Config::IOD->new(
        ignore_unknown_directives => 1,
    );

    die "Unknown constructor argument(s): ".join(", ", sort keys %args)
        if keys %args;
    bless {
        iod => $iod,
        path => $path,
        section => $section,
    }, $class;
}

sub dump {
    require File::Flock::Retry;

    my ($kvstore) = @_;

    my $lock = File::Flock::Retry->lock($kvstore->{path});
    my $doc = $kvstore->{iod}->read_file($kvstore->{path});
    $lock->release;

    my  %vals;
    $doc->each_key(
        sub {
            my (undef, %cbargs) = @_;
            next unless $cbargs{section} eq $kvstore->{section};
            $vals{ $cbargs{key} } = $cbargs{raw_value} + 0;
        });

    \%vals;
}

sub get {
    require File::Flock::Retry;

    my ($self, %args) = @_;

    my $key = delete($args{key});
    defined $key or die "Please specify key";
    die "Unknown constructor argument(s): ".join(", ", sort keys %args)
        if keys %args;

    my $lock = File::Flock::Retry->lock($self->{path});
    my $doc = $self->{iod}->read_file($self->{path});
    $lock->release;

    $doc->get_value($self->{section}, $key);
}

sub set {
    require File::Flock::Retry;
    require File::Slurper;

    my ($self, %args) = @_;

    my $key = delete($args{key});
    defined $key or die "Please specify key";
    my $val = delete($args{value});
    defined $val or die "Please specify value"; # we currently cannot store undef or refs
    my $dry_run = delete($args{-dry_run});
    die "Unknown constructor argument(s): ".join(", ", sort keys %args)
        if keys %args;

    my $lock = File::Flock::Retry->lock($self->{path});
    my $doc = $self->{iod}->read_file($self->{path});
    my $oldval;
    if ($doc->key_exists($self->{section}, $key)) {
        $oldval = $doc->get_value($self->{section}, $key);
        $doc->set_value({create_section=>1}, $self->{section}, $key, $val)
            unless $dry_run;
    } else {
        $doc->insert_key({create_section=>1}, $self->{section}, $key, $val)
            unless $dry_run;
    }

    File::Slurper::write_binary($self->{path}, $doc->as_string);
    $lock->release;

    $val;
}

our %SPEC;

$SPEC{':package'} = {
    v => 1.1,
    summary => 'A simple key-value store using IOD/INI file',
    description => <<'_',

This module provides simple key-value store using IOD/INI file as the backend.
You can get or set value using a single function call or a single CLI script
invocation.

Currently undef and reference values are not supported.

_
};

our %argspecs_common = (
    path => {
        summary => 'IOD/INI file',
        description => <<'_',

If not specified, will default to $HOME/kvstore.iod. If file does not exist,
will be created.

_
        schema => 'filename*',
        pos => 1,
    },
    section => {
        summary => 'INI section name where the values are put',
        schema => 'str*',
        default => 'keyvaluestore',
        description => <<'_',

Key-value pairs are put as parameters in a specific section in the IOD/INI file,
e.g.:

    [keyvaluestore]
    key1=val1
    key2=val2

This argument customizes the section name.

_
    },
);

our %argspec_key = (
    key => {
        summary => 'Key name',
        description => <<'_',

Note that key name must be valid IOD/INI parameter name.

_
        schema => 'str*',
        pos => 0,
        req => 1,
    },
);

our %argspec_value = (
    value => {
        summary => 'Value',
        description => <<'_',

_
        schema => 'str*',
        pos => 1,
        req => 1,
    },
);

$SPEC{set_iod_kvstore_value} = {
    v => 1.1,
    summary => 'Set a value in an IOD/INI key-value store file and return the old value',
    description => <<'_',

_
    args => {
        %argspecs_common,
        %argspec_key,
        %argspec_value,
    },
    features => {
        dry_run => 1,
    },
};
sub set_iod_kvstore_value {
    my %args = @_;

    my $obj = __PACKAGE__->new(
        path => $args{path},
        section => $args{section},
    );
    [200, "OK",
     $obj->set(key => $args{key}, value => $args{value}, -dry_run=>$args{-dry_run})];
}

$SPEC{dump_iod_kvstore} = {
    v => 1.1,
    summary => 'Return all the value in the IOD/INI key-value store file as a hash',
    description => <<'_',
_
    args => {
        %argspecs_common,
    },
};
sub dump_iod_kvstore {
    my %args = @_;

    my $obj = __PACKAGE__->new(
        path => $args{path},
        section => $args{section},
    );
    [200, "OK",
     $obj->dump()];
}

$SPEC{get_iod_kvstore_value} = {
    v => 1.1,
    summary => 'Get the value of a key in an IOD/INI key-value store file',
    description => <<'_',
_
    args => {
        %argspecs_common,
        %argspec_key,
    },
};
sub get_iod_kvstore_value {
    my %args = @_;

    my $obj = __PACKAGE__->new(
        path => $args{path},
        section => $args{section},
    );

    my $val = $obj->get(key => $args{key});
    [200, "OK", $val];
}

1;
# ABSTRACT:

=head1 SYNOPSIS

From Perl:

 use IOD::KeyValueStore::Simple qw(
     dump_iod_kvstore
     get_iod_kvstore_value
     set_iod_kvstore_value
 );

 XXX


=head1 METHODS

Aside from the functional interface, this module also provides the OO interface.

=head2 new

Constructor.

Usage:

 my $kvstore = IOD::KeyValueStore::Simple->new(%args);

Known arguments (C<*> marks required argument):

=over

=item * path

IOD file path, defaults to C<$HOME/kvstore.iod>.

=back

=head2 dump

Return all key-value pairs as a hash.

Usage:

 my $hash = $kvstore->dump;

=head2 get

Get the value of a key. Returns undef if key does not exist.

Usage:

 my $val = $->increment(%args);

Arguments:

=over

=item * counter

Counter name, defaults to C<default>.

=item * increment

Increment, defaults to 1.

=back

=head2 get

Get current value of a counter.

Usage:

 my $val = $counter->get(%args);

Arguments:

=over

=item * counter

Counter name, defaults to C<default>.

=back

=head2 dump

Dump all counters as a hash.

Usage:

 my $counters = $counter->dump(%args);

Arguments:

=over

=back


=head1 SEE ALSO

L<SQLite::Counter::Simple>
