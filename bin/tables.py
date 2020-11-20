import logging


class Printer():

    def __init__(self):
        self.header = []
        self.columns = 0
        self.rows = 0
        self.lengths = {}
        self.justification = {}
        self.data = {}

    def clear(self):
        """ Clear, start over """
        self.header = []
        self.columns = 0
        self.rows = 0
        self.lengths = {}
        self.data = {}
        return

    def add_data(self, row_values, key=None):
        return self.add_row(row_values, key)

    def add_row(self, row_values, key=None):
        """ Add a row of data, with an optional key for sorting """
        if len(row_values) != self.columns:
            logging.error("add_row: failed, row_values=%d != columns=%d!",
                          len(row_values), self.columns)
            return False
        self.update_lengths(row_values)
        if not key:
            self.data[str(self.rows)] = row_values
            self.rows += 1
        else:
            self.data[key] = row_values
        return self.rows

    def update_lengths(self, row_values):
        """ Update column widths for given row values to max """
        for v in row_values:
            vl = len(str(v))
            idx = row_values.index(v)
            hdr = self.header[idx]
            if self.lengths.get(hdr, vl) <= vl:
                self.lengths[hdr] = vl
        return

    def add_header(self, name, justification="<", length=None):
        """ Build the header one column at a time, specifying justification """
        self.header.append(name)
        if length:
            self.lengths[name] = length
        else:
            self.lengths[name] = len(name)
        self.justification[name] = justification
        return len(self.header)

    def set_header(self, header, justification="<"):
        """ set the header from a list all at once """
        if type(header) != list:
            logging.error("add_header: failed, header must be a list!")
            return False
        self.header = header
        self.columns = len(header)
        for h in header:
            self.lengths[h] = len(h)
            self.justification[h] = justification
        self.data = {}
        return

    def dump(self, sort=True, reverse=False, header_underline=False, padding="  "):
        """ Dump the output """
        if header_underline and padding == "  ":
            padding = " | "
        output = self.dump_header(header_underline, padding)
        output += self.dump_data(sort, reverse, padding)
        return output

    def dump_header(self, header_underline=False, padding="  "):
        """ Dump out just the header """
        fields = []
        for h in self.header:
            idx = self.header.index(h)
            fields.append("{%d:%s%ds}" % (idx, self.justification[h],
                                          self.lengths[h]))

        format_line = padding.join(fields)
        line = format_line.format(*self.header)
        width = len(line)
        output = "%s\n" % line
        if header_underline:
            output += "-" * width + "\n"
        return output

    def dump_data(self, sort=True, reverse=False, padding="  "):
        """ Dump out just the data """
        output = ""
        data_keys = self.data.keys()
        if sort:
            data_keys.sort(reverse=reverse)
        for k in data_keys:
            fields = []
            for d in self.data[k]:
                idx = self.data[k].index(d)
                hdr = self.header[idx]
                fields.append("{%d:%s%ds}" % (idx, self.justification[hdr],
                                              self.lengths[hdr]))

            format_line = padding.join(fields)
            line = format_line.format(*self.stringify(self.data[k]))
            output += "%s\n" % line
        return output

    def stringify(self, input_list):
        """ Convert a list of X to a list of strings """
        return [str(i) for i in input_list]
