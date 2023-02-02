

class TestCustomSnapshot:

    def update_project_config(self):
        """
        update project config
        run the same tests that were in TestSimpleSnapshot using custom snapshot
        """


class TestNamespacedCustomSnapshot:

    def update_project_config(self):
        """
        update project config
        run the same tests that were in TestSimpleSnapshot using a namespaced custom snapshot
        """


class TestInvalidNamespacedCustomSnapshot:

    def update_project_config(self):
        """
        update project config
        run the same tests that were in TestSimpleSnapshot using a non-existent namespaced custom snapshot
        expect a failed run
        """
