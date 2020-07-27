from sphinx import addnodes
from sphinx.directives import ObjectDescription
from sphinx.domains import Domain


class OGMPropertyDirective(ObjectDescription):

    has_content = True

    def handle_signature(self, sig, signode):
        signode += addnodes.desc_annotation(text="property ")
        signode += addnodes.desc_name(text=sig)
        return sig


class OGMLabelDirective(ObjectDescription):

    has_content = True

    def handle_signature(self, sig, signode):
        signode += addnodes.desc_annotation(text="label ")
        signode += addnodes.desc_name(text=sig)
        return sig


class OGMRelatedDirective(ObjectDescription):

    has_content = True

    def handle_signature(self, sig, signode):
        signode += addnodes.desc_annotation(text="related ")
        signode += addnodes.desc_name(text=sig)
        return sig


class OGMDomain(Domain):

    name = "ogm"
    directives = {
        "property": OGMPropertyDirective,
        "label": OGMLabelDirective,
        "related": OGMRelatedDirective,
    }


def setup(app):
    app.add_domain(OGMDomain)

    return {
        'version': '0.1',
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
