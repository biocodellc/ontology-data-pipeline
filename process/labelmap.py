"""
Provides a single class, LabelMap, that implements methods for mapping term
labels to term IRIs in a given ontology.  A major challenge with using labels
to identify terms is deciding how to deal with label collisions, which are
nearly inevitable given a large enough imports closure.  LableMap takes the
approach of issuing non-fatal warnings when collisions are encountered while
building the label lookup table.  However, LabelMap keeps track of all known
ambiguous labels, and if client code attempts to look up the IRI for an
ambiguous label, an exception is thrown.
"""

# Python imports.
import logging
import requests
from lxml import etree


class LabelMap:
    """
    Maintains a lookup table for an ontology that maps term labels to their
    associated term IRIs.
    """

    def __init__(self, ontology):
        """
        Initializes this LabelMap from an ontology.

        ontology: An OWL ontology filepath. Can also be a URL
        """
        # Dictionary for the labels lookup table.
        self.lmap = {}

        # Dictionary to keep track of ambiguous labels and the IRIs to which
        # they refer.
        self.ambiglabels = {}

        self.ontology = ontology

        self.__makeMap()

    def lookupIRI(self, label, IRI_root=''):
        """
        Retrieve the IRI associated with a given term label.  If IRI_root is
        provided, it will be used to confirm the retrieved IRI and, in the case
        of ambiguous labels (i.e., labes associated with more than one IRI), it
        will be used to attempt to disambiguate the label reference.  If the
        label (possibly with an IRI_root) is ambiguous, an exception is thrown.

        label: A label string.
        IRI_root: A string containing the root of the term IRI.
        Returns: The OWl API IRI object associated with the label.
        """
        if label not in self.ambiglabels:
            labelIRI = self.lmap[label]
            if str(labelIRI).startswith(IRI_root):
                return labelIRI
            else:
                raise RuntimeError(
                    'The provided IRI root, <' + IRI_root
                    + '>, does not match the IRI associated with the label "'
                    + label + '" (<' + str(labelIRI) + '>.'
                )
        else:
            # Check if IRI_root can disambiguate the label reference.
            lastmatch = None
            matchcnt = 0
            for labelIRI in self.ambiglabels[label]:
                if str(labelIRI).startswith(IRI_root):
                    lastmatch = labelIRI
                    matchcnt += 1

            if matchcnt == 1:
                return lastmatch
            else:
                raise RuntimeError(
                    'Attempted to use an ambiguous label: The label "' + label
                    + '" is used for multiple terms in the ontology <'
                    + str(self.ontology.getOntologyID().getOntologyIRI().get())
                    + '>, including its imports closure.  The label "' + label
                    + '" is associated with the following IRIs: ' + '<'
                    + '>, <'.join([str(labelIRI) for labelIRI in self.ambiglabels[label]])
                    + '>.'
                )

    def add(self, label, termIRI):
        """
        Adds an IRI/label pair to this LabelMap.  If the label is already in
        use in the ontology, a warning is issued.

        label: A string containing the label text.
        termIRI: An OWl API IRI object.
        """
        if label not in self.lmap:
            self.lmap[label] = termIRI
        else:
            if not (self.lmap[label].equals(termIRI)):
                self.__addAmbiguousLabel(label, termIRI)
                logging.warning(
                    'The label "' + label +
                    '" is used for more than one IRI in the provided ontology.'
                    + ', including its imports closure.  The label "' + label
                    + '" is associated with the following IRIs: ' + '<'
                    + '>, <'.join([str(labelIRI) for labelIRI in self.ambiglabels[label]])
                    + '>.'
                )

    def __addAmbiguousLabel(self, label, termIRI):
        """
        Adds a label, along with its term IRI, to the set of ambiguous labels.
        """
        if label not in self.ambiglabels:
            self.ambiglabels[label] = [self.lmap[label], termIRI]
        else:
            self.ambiglabels[label].append(termIRI)

    def __makeMap(self):
        """
        Creates label lookup table entries for a given ontology, including its
        imports closure, that map class labels (i.e., the values of rdfs:label
        axioms) to their corresponding class IRIs.  This function verifies that
        none of the labels are ambiguous; that is, that no label is used for
        more than one IRI.  If an ambiguous label is encountered, a warning is
        issued.
        """
        if self.ontology.startswith(('http://', 'https://')):
            response = requests.get(self.ontology)

            if response.status_code is not 200:
                raise RuntimeError("Error fetching ontology")

            root = etree.fromstring(response.content)
        else:
            root = etree.parse(self.ontology).getroot()

        self.__importLabels(root)

        for ontology in root.findall('owl:Ontology', root.nsmap):
            for ontology_import in ontology.findall('owl:imports', root.nsmap):
                path = ontology_import.attrib["{{{}}}resource".format(root.nsmap['rdf'])]
                response = requests.get(path)

                if response.status_code is not 200:
                    raise RuntimeError("Failed to fetch external resource {}".format(path))

                self.__importLabels(etree.fromstring(response.content).getroot())

    def __importLabels(self, root):
        for annotation_axiom in root.findall('owl:Class', root.nsmap):
            label = annotation_axiom.find('rdfs:label', annotation_axiom.nsmap)
            if label and label.text:
                self.add(label.text, annotation_axiom.attrib["{{{}}}about".format(annotation_axiom.nsmap['rdf'])])
