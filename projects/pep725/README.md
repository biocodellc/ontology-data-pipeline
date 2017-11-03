# PEP725 table
I've exported the main tables as csv for you:
pep725_data ... list of observations, main identifier for the plant is
the gss_id, for the phase the phase_id.
Additionally you can find genus_id, species_id, subspecies_id - they are
redundant information to the gss_id but it speeds up any request which
is only interested on records at the genus level. species_id and
subspecies_id might be empty as not all observers know the exact species
or even subspecies.
pep725_season ... flag to separate summer and winter cereals

pep725_genus ... description of genus_id
pep725_species ...description of species_id
pep725_subspecies ...description of subspec_id

pep725_stations ... description of the observation locations

pep725_phase .... description of the plant development stages, up to
phase_id 99 we follow the BBCH schema, above we've defined everything
else which doesn't fit into BBCH

I've skipped the two data columns: affected_flag and qc_flag as both are
still in development (affected should contain in future some information
about recurring phases, anomalies due to frost or pests,...), qc_flag
should contain something from our quality checks.
