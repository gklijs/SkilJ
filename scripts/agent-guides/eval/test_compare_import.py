"""Tests of compare_import.py, against what this workspace's skilj-codegen
imports from import-model's model.

Run: python3 -m unittest scripts/agent-guides/eval/test_compare_import.py
(builds skilj-codegen the way lib.sh does, if it isn't built yet).
"""

import os
import subprocess
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, HERE)

import compare_import  # noqa: E402

MODEL = os.path.join(HERE, "fixtures", "import-model", "models", "savings.em.json")
TOOLS = os.path.join(ROOT, "target", "agent-eval-tools")


def imported():
    subprocess.run(
        ["cargo", "build", "--quiet", "-p", "skilj-codegen", "--target-dir", TOOLS],
        cwd=ROOT,
        check=True,
    )
    result = subprocess.run(
        [os.path.join(TOOLS, "debug", "skilj-codegen"), "import", "--from", "em-json", MODEL, "savings"],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


class CompareImport(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.imported = imported()

    def problems(self, project):
        return compare_import.compare(self.imported, project)

    def test_the_import_itself_passes(self):
        self.assertEqual(self.problems(self.imported), [])

    def test_the_import_has_what_the_tests_take_out(self):
        self.assertIn('tags = { goal_id = "goal_id" }', self.imported)
        self.assertIn("# Rejected as: goal_closed, goal_not_found", self.imported)
        self.assertIn('{ name = "target_amount", type = "i64" }', self.imported)

    def test_an_addition_passes(self):
        project = self.imported.replace(
            '    { name = "target_amount", type = "i64" },\n',
            '    { name = "target_amount", type = "i64" },\n    { name = "note", type = "string", optional = true },\n',
        ) + '\n[[event_type]]\nname = "GoalRenamed"\nfields = [\n    { name = "goal_id", type = "string" },\n]\n'
        self.assertNotEqual(project, self.imported)
        self.assertEqual(self.problems(project), [])

    def test_a_removed_goal_tag_fails(self):
        project = self.imported.replace('tags = { goal_id = "goal_id" }\n', "", 1)
        problems = self.problems(project)
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("tag goal_id", problems[0])

    def test_a_removed_rejection_annotation_fails(self):
        project = self.imported.replace("# Rejected as: goal_closed, goal_not_found\n", "", 1)
        problems = self.problems(project)
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("SaveTowardsGoal", problems[0])
        self.assertIn("goal_closed, goal_not_found", problems[0])

    def test_a_moved_rejection_annotation_passes(self):
        project = self.imported.replace(
            '# Rejected as: goal_closed, goal_not_found\n[[command_type]]\n# From Close Goal (5100000000000000031)\nname = "CloseGoal"\n',
            '[[command_type]]\nname = "CloseGoal"\n# Rejected as: goal_closed, goal_not_found\n',
        )
        self.assertNotEqual(project, self.imported)
        self.assertEqual(self.problems(project), [])

    def test_a_renamed_field_fails(self):
        project = self.imported.replace('{ name = "amount", type = "i64" }', '{ name = "sum", type = "i64" }')
        problems = self.problems(project)
        self.assertTrue(problems)
        self.assertTrue(all("field amount is missing" in problem for problem in problems), problems)

    def test_a_retyped_field_fails(self):
        project = self.imported.replace('{ name = "amount", type = "i64" }', '{ name = "amount", type = "string" }')
        self.assertTrue(any("field amount is string" in problem for problem in self.problems(project)))

    def test_another_bounded_context_fails(self):
        project = self.imported.replace('bounded_context = "savings"', 'bounded_context = "goals"')
        self.assertTrue(any("bounded_context" in problem for problem in self.problems(project)))


if __name__ == "__main__":
    unittest.main()
